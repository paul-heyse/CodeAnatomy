# CQ Tree-sitter-First Enriched Context Hard Cutover Plan v1 (2026-02-14)

## Scope
Define and execute a hard cutover to a tree-sitter-first enriched context pipeline for `cq search` and `cq neighborhood`, using:
- `tree-sitter` core runtime primitives,
- `tree-sitter-python` and `tree-sitter-rust` grammar bundles,
- query-pack engineering (`highlights`/`locals`/`injections`/`tags` patterns),
- bounded/incremental query execution,
- `node-types.json`-driven schema validation,
- request-scoped msgspec contracts and caches.

This plan is intentionally aggressive for design phase: no compatibility layers, no legacy dual-path operation retained after cutover.

## Documentation Basis Reviewed End-to-End
- `docs/python_library_reference/tree_sitter_neighborhood_creation.md`
- `docs/python_library_reference/tree-sitter.md`
- `docs/python_library_reference/tree-sitter_advanced.md`
- `docs/python_library_reference/tree-sitter-rust.md`

## Hard Constraints
1. Query-driven only: no full-repo scan/caching for default search execution.
2. Cache only for repeated work inside a request, or for artifact reuse when input fingerprints prove validity.
3. Tree-sitter is the primary structural evidence plane for enriched context.
4. Python native (`ast`/`symtable`) becomes secondary evidence only where tree-sitter cannot represent a fact directly.
5. Output remains object-resolved: one code-facts summary per object, many occurrences.

## Findings (Current State)
1. Neighborhood construction is still ast-grep structural-first.
- `tools/cq/neighborhood/structural_collector.py`
- `tools/cq/neighborhood/scan_snapshot.py`
- `tools/cq/neighborhood/bundle_builder.py`

2. Tree-sitter Python is currently a late-stage gap-fill, not a primary context builder.
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/python_enrichment.py`

3. Query-pack governance is shallow and Python-only.
- `tools/cq/search/query_pack_lint.py` only calls Python lint and does not enforce rooted/local/windowable constraints across Python and Rust packs.

4. Rust tree-sitter enrichment extracts useful local facts, but does not yet operate as a query-pack-driven indexing substrate (defs/refs/calls/types/attrs/docs/injections/tags contracts).
- `tools/cq/search/tree_sitter_rust.py`

5. Search output quality gaps (N/A-heavy facts) indicate object-level fact attachment is underpowered and not consistently backed by strong structural evidence.
- `tools/cq/search/object_resolver.py`
- `tools/cq/search/smart_search.py`

## Tree-sitter Capabilities to Exploit (From Reference Docs)
1. Precise anchor resolution:
- `named_descendant_for_point_range`, `named_descendant_for_byte_range`
- field-aware parent/child labeling via `field_name_for_child`, `child_by_field_name`

2. Bounded query execution:
- `QueryCursor.set_byte_range`, `set_point_range`, `set_max_start_depth`, `match_limit`, `did_exceed_match_limit`, `progress_callback`

3. Incremental parse mechanics:
- `old_tree` parse reuse, `tree.edit(...)`, `changed_ranges(...)`

4. Projection/injection parsing:
- `Parser(..., included_ranges=[...])` for embedded language extraction while preserving host byte coordinates.

5. Query compilation and lint primitives:
- `is_pattern_rooted`, `is_pattern_non_local`, `capture_quantifier`, `pattern_settings`, `pattern_assertions`

6. Grammar schema contracts:
- `node-types.json` as static ABI for node kinds/fields/children/supertypes.

7. Rust grammar bundle model:
- load `tree-sitter.json` + `queries/*.scm` + `src/node-types.json`
- leverage tags/injections packs and macro-aware extraction surfaces.

## Target End State Architecture

### Stage A: Targeted candidate boundary
`ripgrep -> ast-grep -> file/byte windows`
- `rg` limits candidate files.
- ast-grep pinpoints candidate nodes.
- only those files/windows enter tree-sitter pipeline.

### Stage B: Tree-sitter parse session (request scoped)
- one parse session per candidate file/language.
- incremental reparse if same file is revisited inside request.
- changed-range scoped query reruns for repeated operations.

### Stage C: Query-pack execution
- run curated Python/Rust packs for:
  - defs
  - refs
  - calls
  - imports/modules
  - locals scopes
  - structural role/context
  - parse quality (`ERROR`/`MISSING`)
  - injections
  - tags (Rust bootstrap lane)

### Stage D: Object resolution + neighborhood graph assembly
- resolve object identity using tree-sitter-first evidence.
- build per-object enriched facts from representative anchor + aggregated per-occurrence structural context.
- assemble neighborhood slices from CST relationships:
  - parents
  - children
  - siblings
  - statement siblings
  - conjunction peers
  - field-labeled edges

### Stage E: Artifact-first diagnostics
- terminal: concise object + occurrence location view.
- artifacts: full snippets, diagnostics, query telemetry, structural/event tables.

## Workstream Implementation Plan

## W1. Shared Tree-sitter Runtime Core
Create a single bounded execution runtime used by Python and Rust.

Representative pattern:
```python
@dataclass(frozen=True)
class QueryWindow:
    start_byte: int
    end_byte: int

@dataclass(frozen=True)
class QueryRunResult:
    matches: list[tuple[int, dict[str, Node]]]
    exceeded_match_limit: bool
    cancelled: bool


def run_bounded_query(
    query: Query,
    root: Node,
    windows: list[QueryWindow],
    *,
    match_limit: int,
    max_start_depth: int | None,
    progress_cb: Callable[[object], bool] | None,
) -> QueryRunResult:
    cursor = QueryCursor(query, match_limit=match_limit)
    if max_start_depth is not None:
        cursor.set_max_start_depth(max_start_depth)
    out: list[tuple[int, dict[str, Node]]] = []
    cancelled = False
    for w in windows:
        cursor.set_byte_range(w.start_byte, w.end_byte)
        batch = cursor.matches(root, progress_callback=progress_cb)
        out.extend(batch)
        if cursor.did_exceed_match_limit:
            break
    return QueryRunResult(out, bool(cursor.did_exceed_match_limit), cancelled)
```

Target files to edit:
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/python_enrichment.py`

New files to create:
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/tree_sitter_runtime_contracts.py`

## W2. Grammar Schema + Query-Pack Contract System
Enforce query packs against `node-types.json` and query introspection gates.

Representative pattern:
```python
class NodeTypeSpec(msgspec.Struct, frozen=True):
    type: str
    named: bool
    fields: dict[str, object]
    children: object | None = None
    subtypes: list[dict[str, object]] | None = None


def lint_query_pack(query: Query, schema: GrammarSchema) -> list[str]:
    errors: list[str] = []
    for i in range(query.pattern_count):
        if not query.is_pattern_rooted(i):
            errors.append(f"pattern {i}: not rooted")
        if query.is_pattern_non_local(i):
            errors.append(f"pattern {i}: non-local")
    return errors
```

Target files to edit:
- `tools/cq/search/query_pack_lint.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`

New files to create:
- `tools/cq/search/tree_sitter_node_schema.py`
- `tools/cq/search/tree_sitter_query_contracts.py`
- `tools/cq/search/tree_sitter_query_registry.py`
- `tools/cq/search/queries/python/contracts.yaml`
- `tools/cq/search/queries/rust/contracts.yaml`

## W3. Python Enriched Facts: Query-Pack-First
Promote tree-sitter Python from gap-fill to primary structural fact source.

Representative pattern:
```python
# locals + defs + calls + imports + structure
captures = run_query_pack_set(
    lang="python",
    packs=("00_defs", "10_refs", "20_calls", "30_imports", "40_locals", "50_structure"),
    source_bytes=src,
    windows=windows,
)

facts = build_python_structural_facts(
    captures=captures,
    anchor=anchor_node,
    include_parse_quality=True,
)
```

Target files to edit:
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/python_enrichment.py`
- `tools/cq/search/object_resolver.py`

New files to create:
- `tools/cq/search/tree_sitter_python_facts.py`
- `tools/cq/search/queries/python/00_defs.scm`
- `tools/cq/search/queries/python/10_refs.scm`
- `tools/cq/search/queries/python/20_calls.scm`
- `tools/cq/search/queries/python/30_imports.scm`
- `tools/cq/search/queries/python/40_locals.scm`
- `tools/cq/search/queries/python/50_structure.scm`
- `tools/cq/search/queries/python/90_quality.scm`

## W4. Rust Enriched Facts: Bundle-Aware + Injections + Tags
Adopt Rust grammar-bundle semantics directly: manifest, query packs, node schema, tags/injections.

Representative pattern:
```python
def load_rust_bundle() -> RustBundle:
    manifest = read_tree_sitter_manifest("tree-sitter-rust")
    node_schema = load_node_types_json("tree-sitter-rust")
    packs = load_manifest_query_packs(manifest)
    return RustBundle(manifest=manifest, node_schema=node_schema, packs=packs)
```

Rust injections with host byte preservation:
```python
r = Range(content.start_point, content.end_point, content.start_byte, content.end_byte)
parser = Parser(injected_lang, included_ranges=[r])
embedded_tree = parser.parse(host_bytes)
```

Target files to edit:
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/rust_enrichment.py`

New files to create:
- `tools/cq/search/tree_sitter_rust_bundle.py`
- `tools/cq/search/tree_sitter_injections.py`
- `tools/cq/search/tree_sitter_tags.py`
- `tools/cq/search/queries/rust/00_defs.scm`
- `tools/cq/search/queries/rust/10_refs.scm`
- `tools/cq/search/queries/rust/20_calls.scm`
- `tools/cq/search/queries/rust/30_types.scm`
- `tools/cq/search/queries/rust/40_attrs_docs.scm`
- `tools/cq/search/queries/rust/50_modules_imports.scm`
- `tools/cq/search/queries/rust/60_injections.scm`

## W5. Tree-sitter Neighborhood Engine (Replace Structural Collector)
Implement neighborhood directly from CST node graph and field-aware edges.

Representative pattern:
```python
def build_neighborhood(anchor: Node, src: bytes) -> NeighborhoodV1:
    stmt = statement_node(anchor)
    return NeighborhoodV1(
        anchor=node_ref(anchor),
        parents=collect_parents(anchor),
        children=collect_children(anchor),
        siblings=collect_siblings(anchor),
        statement_siblings=collect_siblings(stmt),
        conjunction_peers=collect_conjunction_peers(anchor),
        field_edges=collect_field_edges(anchor),
    )
```

Target files to edit:
- `tools/cq/neighborhood/bundle_builder.py`
- `tools/cq/neighborhood/section_layout.py`
- `tools/cq/neighborhood/snb_renderer.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/search/smart_search.py`

New files to create:
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/neighborhood/tree_sitter_contracts.py`

## W6. Object-Fact Attachment Quality and N/A Reduction
Attach code facts from object-level tree-sitter evidence, and suppress not-applicable noise.

Representative pattern:
```python
def render_fact(value: object, *, applicable: bool) -> str | None:
    if not applicable:
        return None  # omit from terminal entirely
    if value is None:
        return "N/A - not resolved"
    return str(value)
```

Target files to edit:
- `tools/cq/search/object_resolver.py`
- `tools/cq/search/object_resolution_contracts.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/core/report.py`

New files to create:
- `tools/cq/search/object_fact_attachment.py`

## W7. msgspec-Native Contracts for Tree-sitter Events
Use msgspec structs for intermediate event streams and artifact payloads.

Representative pattern:
```python
class TsEventV1(msgspec.Struct, frozen=True):
    file: str
    language: str
    kind: str
    start_byte: int
    end_byte: int
    start_line: int
    end_line: int
    captures: dict[str, str]
```

Target files to edit:
- `tools/cq/search/object_resolution_contracts.py`
- `tools/cq/core/cache/contracts.py`
- `tools/cq/core/cache/fragment_codecs.py`

New files to create:
- `tools/cq/search/tree_sitter_event_contracts.py`
- `tools/cq/search/tree_sitter_artifact_contracts.py`

## W8. Parallel Execution and Budgeting
Parallelize by file partition and language lane, with deterministic budgets.

Representative pattern:
```python
with ProcessPoolExecutor(max_workers=max_workers) as ex:
    futures = [ex.submit(run_file_lane, file_job) for file_job in file_jobs]
    for f in as_completed(futures):
        merge_lane_result(f.result())
```

Target files to edit:
- `tools/cq/search/partition_pipeline.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/search/semantic_request_budget.py`

New files to create:
- `tools/cq/search/tree_sitter_parallel.py`

## File Plan

### Primary files to edit
- `tools/cq/search/smart_search.py`
- `tools/cq/search/python_enrichment.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/query_pack_lint.py`
- `tools/cq/search/object_resolver.py`
- `tools/cq/search/object_resolution_contracts.py`
- `tools/cq/neighborhood/bundle_builder.py`
- `tools/cq/neighborhood/section_layout.py`
- `tools/cq/neighborhood/snb_renderer.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/core/report.py`
- `tools/cq/core/cache/contracts.py`
- `tools/cq/core/cache/fragment_codecs.py`

### New files to create
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/tree_sitter_runtime_contracts.py`
- `tools/cq/search/tree_sitter_node_schema.py`
- `tools/cq/search/tree_sitter_query_contracts.py`
- `tools/cq/search/tree_sitter_query_registry.py`
- `tools/cq/search/tree_sitter_python_facts.py`
- `tools/cq/search/tree_sitter_rust_bundle.py`
- `tools/cq/search/tree_sitter_injections.py`
- `tools/cq/search/tree_sitter_tags.py`
- `tools/cq/search/tree_sitter_event_contracts.py`
- `tools/cq/search/tree_sitter_artifact_contracts.py`
- `tools/cq/search/object_fact_attachment.py`
- `tools/cq/search/tree_sitter_parallel.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/neighborhood/tree_sitter_contracts.py`
- `tools/cq/search/queries/python/contracts.yaml`
- `tools/cq/search/queries/rust/contracts.yaml`
- `tools/cq/search/queries/python/00_defs.scm`
- `tools/cq/search/queries/python/10_refs.scm`
- `tools/cq/search/queries/python/20_calls.scm`
- `tools/cq/search/queries/python/30_imports.scm`
- `tools/cq/search/queries/python/40_locals.scm`
- `tools/cq/search/queries/python/50_structure.scm`
- `tools/cq/search/queries/python/90_quality.scm`
- `tools/cq/search/queries/rust/00_defs.scm`
- `tools/cq/search/queries/rust/10_refs.scm`
- `tools/cq/search/queries/rust/20_calls.scm`
- `tools/cq/search/queries/rust/30_types.scm`
- `tools/cq/search/queries/rust/40_attrs_docs.scm`
- `tools/cq/search/queries/rust/50_modules_imports.scm`
- `tools/cq/search/queries/rust/60_injections.scm`

## Full Decommission and Delete Plan (Legacy Artifacts)
Delete these modules and references after cutover completion:
1. `tools/cq/neighborhood/structural_collector.py`
2. `tools/cq/neighborhood/scan_snapshot.py`
3. `tools/cq/neighborhood/cache_pipeline.py` (only if no remaining non-neighborhood consumers)
4. `smart_search` structural neighborhood preview path:
- `_build_structural_neighborhood_preview` and all call sites.
5. Python tree-sitter gap-fill-only behavior in `tools/cq/search/tree_sitter_python.py`:
- remove "best-effort additive only" constraints that block primary fact ownership.
6. Any duplicated query packs superseded by new numbered packs:
- `tools/cq/search/queries/python/enrichment_core.scm`
- `tools/cq/search/queries/python/enrichment_locals.scm`
- `tools/cq/search/queries/python/enrichment_imports.scm`
- `tools/cq/search/queries/python/enrichment_resolution.scm`
- `tools/cq/search/queries/python/enrichment_bindings.scm`

## Execution Sequence
1. Implement W1 runtime core and W2 lint/contracts first.
2. Implement W3 Python pack migration and wire object facts.
3. Implement W4 Rust bundle + packs + injections/tags.
4. Implement W5 neighborhood engine and switch CLI/search preview to tree-sitter collector.
5. Implement W6 rendering/output quality cleanup and N/A suppression rules.
6. Implement W7 contractization and cache payload upgrades.
7. Implement W8 parallel budgeting and final performance hardening.
8. Remove deprecated structural modules and obsolete query packs.

## Acceptance Criteria
1. Neighborhood and search enriched facts are tree-sitter-primary for Python and Rust.
2. Every occurrence includes:
- precise location: `file:line[:col]`
- code block bounds: `context_start_line`, `context_end_line`
- stable occurrence id.
3. Object summaries show materially fewer unresolved clusters; `not_applicable` fields are omitted from terminal output.
4. Query-pack lint fails on non-rooted/non-local/window-hostile patterns and schema-invalid node/field references.
5. No default-path full-repo scan introduced.
6. Cold `./cq search stable_id` remains in "few seconds" budget range with bounded query telemetry.

## Implementation Checklist
- [ ] Add shared bounded tree-sitter runtime and contracts (W1)
- [ ] Add grammar schema loader and query-pack lint contract gates (W2)
- [ ] Replace Python enrichment with query-pack-first structural fact pipeline (W3)
- [ ] Implement Rust bundle/pack runtime with tags and injections support (W4)
- [ ] Implement tree-sitter neighborhood collector and wire into neighborhood/search preview (W5)
- [ ] Attach object facts from tree-sitter-first evidence and remove terminal not-applicable noise (W6)
- [ ] Convert event/artifact payloads to msgspec structs and codecs (W7)
- [ ] Parallelize per-file tree-sitter lanes with deterministic budgets (W8)
- [ ] Delete legacy structural neighborhood modules and old Python enrichment packs
- [ ] Add focused tests under `tests/unit/cq` and `tests/e2e/cq` for all new behavior
