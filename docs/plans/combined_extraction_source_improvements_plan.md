# Combined Extraction Source Improvements Plan

## Goals
- Make source extraction streaming-first and DataFusion-driven.
- Reduce per-file memory overhead and redundant hashing.
- Improve robustness with parser manifests, rich error diagnostics, and deterministic configs.
- Leverage advanced LibCST/AST/tree-sitter capabilities for better performance and fidelity.
- Standardize extract output writes with DataFusion/PyArrow/Delta streaming.

## Inputs
- `docs/plans/extraction_source_improvements.md`
- Additional improvement opportunities identified in extraction review (LibCST/AST/tree-sitter/DataFusion/PyArrow).

## Preconditions / gates
- `scripts/bootstrap_codex.sh` then `uv sync`
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`

## Scope 1: Repo manifest-first scan + blob separation
Objective: keep repo scanning cheap by default, store bytes/text in a separate blob table,
and reuse hashes when file stats are unchanged.

Representative code
```python
# src/extract/repo_scan.py
def _sha256_path(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()

def _build_repo_file_row(rel: Path, repo_root: Path, options: RepoScanOptions) -> dict[str, object]:
    st = abs_path.stat()
    return {
        "path": rel.as_posix(),
        "abs_path": str(abs_path),
        "size_bytes": int(st.st_size),
        "mtime_ns": int(st.st_mtime_ns),
        "file_sha256": _sha256_path(abs_path) if options.include_sha256 else None,
    }
```

Target files
- [x] `src/extract/repo_scan.py`
- [x] `src/extract/repo_scan_git.py`
- [x] `src/extract/repo_scan_fs.py`
- [x] `src/extract/repo_blobs.py`
- [x] `src/datafusion_engine/extract_templates.py`
- [ ] `src/extract/helpers.py`
- [x] `src/hamilton_pipeline/modules/extraction.py`

Implementation checklist
- [x] Make manifest-only scan the default (no bytes/text).
- [x] Implement git-first file listing and `os.walk` pruning fallback.
- [x] Add `mtime_ns` + `size_bytes` to manifest rows.
- [ ] Reuse hashes where unchanged (wire hash index into repo scan).
- [x] Add a separate blob table (bytes/text) for on-demand loading.

Legacy decommission
- [x] Direct `read_bytes()` for hashing in repo scan.
- [x] Manifest rows that always embed `bytes`/`text`.

## Scope 2: DataFusion worklists with streaming consumption
Objective: compute per-extractor worklists in DataFusion and stream file contexts to
extractors without full materialization.

Representative code
```python
# src/extract/worklists.py
def worklist_stream(ctx: SessionContext, sql: str) -> Iterable[FileContext]:
    df = ctx.sql(sql)
    reader = pa.RecordBatchReader.from_batches(df.schema(), df.collect())
    for batch in reader:
        for row in batch.to_pylist():
            yield FileContext.from_repo_row(row)
```

Target files
- [x] `src/extract/worklists.py`
- [x] `src/extract/ast_extract.py`
- [x] `src/extract/cst_extract.py`
- [x] `src/extract/tree_sitter_extract.py`
- [x] `src/extract/bytecode_extract.py`
- [x] `src/extract/symtable_extract.py`
- [x] `src/hamilton_pipeline/modules/extraction.py`
- [x] `src/datafusion_engine/schema_registry.py`

Implementation checklist
- [x] Build per-extractor worklists via DataFusion joins (missing/stale rows).
- [x] Stream `FileContext` from Arrow batches (no `pa.table(df)`).
- [x] Promote `file_sha256` to a top-level column in extract outputs.
- [x] Add worklist SQL templates per dataset (AST/CST/tree-sitter/bytecode/symtable).

Legacy decommission
- [x] Materialized worklists (`pa.table(df)` full collection).
- [x] Hash checks stored only inside `attrs` maps.

## Scope 3: Batch emitters + streaming writer for all extract outputs
Objective: make every extractor produce batch iterators and write outputs without
concat/full tables.

Representative code
```python
# src/extract/batching.py
def record_batches_from_row_batches(schema: pa.Schema, row_batches: Iterable[Sequence[Mapping[str, object]]]):
    for batch in row_batches:
        if batch:
            yield pa.RecordBatch.from_pylist(list(batch), schema=schema)
```

```python
# src/engine/materialize_extract_outputs.py
def write_extract_outputs(name: str, data: Iterable[pa.RecordBatch], *, schema: pa.Schema, location: DatasetLocation):
    ds.write_dataset(data=data, base_dir=str(location.path), format=location.format, schema=schema)
```

Target files
- [ ] `src/extract/batching.py` (new)
- [ ] `src/extract/tree_sitter_extract.py`
- [ ] `src/extract/cst_extract.py`
- [ ] `src/extract/ast_extract.py`
- [ ] `src/engine/materialize_extract_outputs.py` (new)
- [ ] `src/extract/helpers.py`

Implementation checklist
- [x] AST extract already supports `batch_size` + row batching.
- [ ] Add `batch_size` to CST/tree-sitter extract options.
- [ ] Convert row lists to `RecordBatch` iterators (schema-aligned).
- [ ] Replace `write_ast_outputs` with `write_extract_outputs` for all datasets.
- [ ] Route Parquet writes through DataFusion when available (partition/sort).

Legacy decommission
- [ ] `pa.concat_tables` on extractor outputs.
- [ ] AST-only `write_ast_outputs` path.

## Scope 4: Parallel extraction runtime (process-first)
Objective: use process pools for CPU parsing, thread pools only when free-threaded Python,
and pre-warm parser state to reduce per-worker cold start.

Representative code
```python
# src/extract/parallel.py
def parallel_map(items: Iterable[T], fn: Callable[[T], U]) -> Iterable[U]:
    if _gil_disabled():
        with ThreadPoolExecutor() as ex:
            yield from ex.map(fn, items)
        return
    with ProcessPoolExecutor() as ex:
        yield from ex.map(fn, items)
```

Target files
- [ ] `src/extract/parallel.py` (new)
- [ ] `src/extract/cst_extract.py`
- [ ] `src/extract/tree_sitter_extract.py`
- [ ] `src/extract/ast_extract.py`

Implementation checklist
- [ ] Add a shared `parallel_map` helper with GIL detection.
- [ ] Warm LibCST parser before forking (single parse in parent).
- [ ] Resolve FullRepoManager caches before forking to avoid per-worker cost.
- [ ] Use per-worker parser/query pack initialization for tree-sitter.

Legacy decommission
- [ ] Single-threaded, full-repo extraction paths where parallelism is safe.

## Scope 5: LibCST extraction fidelity + manifest enrichment
Objective: maximize LibCST metadata and error diagnostics and normalize parsing config.

Representative code
```python
# src/extract/cst_extract.py
module = cst.parse_module(data)
manifest = {
    "encoding": module.encoding,
    "default_indent": module.default_indent,
    "default_newline": module.default_newline,
    "parser_backend": getattr(module, "config", None).parser_backend,
}
```

Target files
- [ ] `src/extract/cst_extract.py`

Implementation checklist
- [x] Parse bytes-first and persist parse manifest metadata.
- [ ] Capture `ParserSyntaxError.editor_line/editor_column` (context already captured).
- [ ] Use `module.config_for_parsing` for template node creation.
- [ ] Use `FullRepoManager(..., use_pyproject_toml=True)` for correct module roots.
- [ ] Add optional matcher-based extraction for targeted patterns.

Legacy decommission
- [x] Manifest rows without encoding/newline/indent metadata.
- [ ] Module root calculation that ignores pyproject layout.

## Scope 6: AST extraction robustness + manifest parity
Objective: enforce deterministic AST parsing, capture richer error context, and add
manifest rows for AST parsing configuration.

Representative code
```python
# src/extract/ast_extract.py
tree = compile(text, filename, "exec", flags=flags, dont_inherit=True, optimize=optimize)
```

Target files
- [ ] `src/extract/ast_extract.py`

Implementation checklist
- [ ] Default `dont_inherit=True` for deterministic parsing.
- [ ] Enforce `feature_version` from repo metadata when available.
- [ ] Add AST parse manifest rows (mode/type_comments/feature_version/optimize).
- [ ] Capture `SyntaxError.text` context in error rows.
- [ ] Extend `ast.get_source_segment` usage for decorators/defaults/type annotations.

Legacy decommission
- [ ] Parsing paths that inherit ambient `__future__` flags.
- [ ] AST errors without source context details.

## Scope 7: Tree-sitter performance and resilience upgrades
Objective: leverage advanced tree-sitter controls for cancellation, partial parsing,
and large file handling.

Representative code
```python
# src/extract/tree_sitter_extract.py
cursor = QueryCursor(query, match_limit=options.query_match_limit)
cursor.set_byte_range(span.start_byte, span.end_byte)
```

Target files
- [ ] `src/extract/tree_sitter_extract.py`
- [ ] `src/extract/tree_sitter_cache.py`
- [ ] `src/extract/tree_sitter_queries.py`

Implementation checklist
- [ ] Use `progress_callback` for parser/query cancellation (where supported).
- [ ] Add `included_ranges` support to skip generated regions.
- [ ] Support parse callbacks for large files (avoid full read).
- [x] Record grammar ABI + query pack version in file attrs (`tree_sitter_files_v1`).

Legacy decommission
- [ ] Full-file parse for generated/ignored regions.
- [x] Missing query pack version diagnostics.

## Scope 8: Extract output partitioning + DataFusion write policies
Objective: apply partitioning and file sort order consistently for extract outputs to
improve downstream scan performance.

Representative code
```python
# src/datafusion_engine/runtime.py
policy = DataFusionWritePolicy(partition_by=("repo",), sort_by=("path",))
datafusion_write_parquet(df, path=str(location.path), policy=policy)
```

Target files
- [x] `src/engine/materialize.py`
- [ ] `src/datafusion_engine/runtime.py`
- [ ] `src/engine/materialize_extract_outputs.py`
- [ ] `src/ibis_engine/io_bridge.py`

Implementation checklist
- [x] Apply DataFusion write policy + diagnostics for AST outputs.
- [ ] Define partition/sort defaults per dataset (repo/path).
- [ ] Apply DataFusion write policy for non-AST extract outputs.
- [ ] Record write policy metadata in diagnostics for non-AST outputs.

Legacy decommission
- [ ] Parquet writes without partition/sort metadata.

## Scope 9: Bytecode/inspect/symtable quality upgrades
Objective: use advanced stdlib surfaces for improved extraction fidelity.

Representative code
```python
# src/extract/bytecode_extract.py
instructions = dis.get_instructions(co, show_offsets=True, adaptive=options.adaptive)
```

Target files
- [x] `src/extract/bytecode_extract.py`
- [x] `src/extract/runtime_inspect_extract.py`
- [ ] `src/extract/symtable_extract.py`
- [ ] `tests/unit/test_symtable_extract.py` (new)

Implementation checklist
- [x] Capture `Instruction.positions`/`show_offsets` for better span anchors.
- [x] Use `inspect.getmembers_static` to avoid descriptor side effects.
- [ ] Add tests for type-alias/type-parameter scopes in symtable extraction.

Legacy decommission
- [x] Bytecode extractions without instruction position metadata.
- [x] Runtime inspect paths that execute descriptors.

## Global legacy decommission list (after full plan completion)
- `write_ast_outputs` and AST-only output wiring in extraction helpers.
- Any extractor path that materializes full tables before writing.
- Worklist builders that call `pa.table(df)` on DataFusion DataFrames.
- Repo scanning paths that always embed file bytes/text in manifest rows.
