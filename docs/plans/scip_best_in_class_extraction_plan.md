# SCIP Best-in-Class Extraction Plan

## Scope and constraints
- Keep `Document.position_encoding` as the source of truth for ranges (no fallback to metadata).
- Preserve per-document symbol attribution (do not collapse all symbols into index-level only).
- Expose `tool_info.arguments` and project identity values in scip metadata views.
- Fully migrate to flat `scip_*_v1` outputs (no nested `scip_index_v1` output).

## Workstream 1: Metadata + identity enrichment
Goal: surface traceability fields in the scip metadata view and schema, including tool arguments and
project identity (name, version, namespace) where present in the SCIP index.

Target files
- `src/extract/scip_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_introspection.py`

Representative code patterns
```python
# Extract metadata fields, including project identity when present on the Index.
return {
    "protocol_version": _int_or_none(getattr(metadata, "protocol_version", None)),
    "tool_info": {
        "name": _string_value(getattr(tool_info, "name", None)),
        "version": _string_value(getattr(tool_info, "version", None)),
        "arguments": tool_args,
    },
    "project_root": _string_value(getattr(metadata, "project_root", None)),
    "project_name": _string_value(getattr(index, "project_name", None)),
    "project_version": _string_value(getattr(index, "project_version", None)),
    "project_namespace": _string_value(getattr(index, "project_namespace", None)),
}
```

```sql
WITH base AS ({{ nested_base_sql("scip_metadata") }})
SELECT
  base.index_id,
  base.protocol_version,
  base.tool_info["name"] AS tool_name,
  base.tool_info["version"] AS tool_version,
  base.tool_info["arguments"] AS tool_arguments,
  base.project_root,
  base.project_name,
  base.project_version,
  base.project_namespace
FROM base
```

Implementation checklist
- [x] Inspect `scip_pb2.Index` to confirm identity fields and exact names.
- [x] Extend `SCIP_METADATA_SCHEMA` to carry identity values.
- [x] Update `scip_metadata_sql` to expose tool arguments and identity values (pass-through).
- [x] Add schema introspection checks for new columns (information_schema validation).

## Workstream 2: Per-document symbol attribution + signature docs
Goal: preserve `documents.symbols` lineage and surface signature documentation occurrences for
analytics and joinability at the document level.

Target files
- `src/extract/scip_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `tests/unit/test_datafusion_nested_registry.py`

Representative code patterns
```python
row = {
    "document_id": document_id,
    "path": rel_path,
    **_symbol_info_base(symbol_entry, scip_pb2=scip_pb2)[0],
}
```

Implementation checklist
- [x] Add `scip_document_symbols_v1` schema/output mapping with `document_id` + `path`.
- [x] Emit per-document symbol rows in extraction.
- [x] Add `scip_signature_occurrences_v1` dataset for signature doc occurrences.
- [x] Add unit tests for schema expectations (document linkage + role flags).

## Workstream 3: Role/kind decoding + relationship analytics
Goal: expose high-value analytic columns derived from `symbol_roles`, `syntax_kind`, and
`relationships` without requiring consumers to re-implement bitset logic.

Target files
- `src/extract/scip_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `docs/python_library_reference/scip_python_overview.md`

Representative code patterns
```sql
SELECT
  base.symbol,
  base.symbol_roles,
  (bitwise_and(base.symbol_roles, 1) <> 0) AS is_definition,
  (bitwise_and(base.symbol_roles, 2) <> 0) AS is_import,
  (bitwise_and(base.symbol_roles, 4) <> 0) AS is_write,
  (bitwise_and(base.symbol_roles, 8) <> 0) AS is_read,
  CASE base.syntax_kind
    WHEN 1 THEN "Comment"
    WHEN 2 THEN "PunctuationDelimiter"
    ELSE "Unknown"
  END AS syntax_kind_name
FROM base
```

Implementation checklist
- [x] Add decoded role columns in `scip_occurrences_v1` output schema.
- [x] Add relationship analytics via `scip_symbol_relationships_v1` flags.
- [x] Add syntax kind + kind name decoding at extract time.
- [ ] Document the role/kind decoding contract in the SCIP reference doc
      (blocked: do not edit `docs/python_library_reference`).

## Workstream 4: Position encoding guardrails + diagnostics
Goal: keep `Document.position_encoding` as the authoritative source while detecting missing or
inconsistent encodings early.

Target files
- `src/extract/scip_extract.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/query_fragments.py`

Representative code patterns
```python
if doc.position_encoding is None:
    diagnostics["missing_position_encoding"] += 1
```

```sql
SELECT
  COUNT(*) AS missing_posenc
FROM scip_documents
WHERE position_encoding IS NULL
```

Implementation checklist
- [x] Add diagnostics counters for missing `position_encoding`.
- [x] Surface diagnostics payloads via the diagnostics sink.
- [x] Add a normalization guard that logs inconsistent encodings.

## Workstream 5: Document.text retention decision
Goal: decide whether to retain `Document.text` in a flat dataset or make it optional.

Target files
- `src/extract/scip_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`

Representative code patterns
```python
# Toggle document text inclusion via parse options.
include_text = normalized_opts.include_document_text
text = _string_value(getattr(doc, "text", None)) if include_text else None
```

Implementation checklist
- [x] Add telemetry for total text size and row counts to inform the decision.
- [ ] Add a size cap option for `include_document_text`.
- [x] Keep `scip_document_texts_v1` as an optional dataset gate.

## Workstream 6: Scalable extraction path (streaming or flattened outputs)
Goal: avoid full in-memory materialization of all documents/occurrences/symbols for large indexes.

Target files
- `src/extract/scip_extract.py`
- `src/extract/helpers.py`
- `src/datafusion_engine/runtime.py`

Representative code patterns
```python
def iter_scip_documents(index: object) -> Iterator[dict[str, object]]:
    for doc in getattr(index, "documents", []) or []:
        yield _document_entry(doc)

reader = pa.RecordBatchReader.from_pylist(
    iter_scip_documents(index),
    schema=scip_documents_schema,
)
```

Implementation checklist
- [x] Add a fast path that emits flattened tables directly (documents/occurrences/symbols).
- [x] Remove nested `scip_index_v1` output in favor of flat tables.
- [x] Support `prefer_reader=True` by returning RecordBatchReaders for large outputs.
- [x] Add a size-based switch to pick the streaming path automatically.

## Workstream 7: SCIP CLI + environment tooling
Goal: leverage scip-python and scip CLI capabilities to improve determinism and diagnostics.

Target files
- `scripts/gen_scip_env.py` (new)
- `src/extract/scip_indexer.py`
- `src/datafusion_engine/runtime.py`
- `docs/python_library_reference/scip-python_environment_config.md`

Representative code patterns
```python
# scripts/gen_scip_env.py (generator for --environment JSON)
for dist in metadata.distributions():
    env.append(
        {
            "name": dist.metadata.get("Name") or dist.name,
            "version": dist.version,
            "files": sorted({str(p) for p in (dist.files or ())}),
        }
    )
```

```bash
uv run -- python scripts/gen_scip_env.py > build/scip/env.json
uv run -- scip-python index . --project-name ... --environment build/scip/env.json
scip print --json index.scip > build/scip/index.print.json
scip snapshot --comment-syntax "#" index.scip --output build/scip/snapshots
scip test --check-documents index.scip
```

Implementation checklist
- [x] Add `scripts/gen_scip_env.py` and document usage in the env config guide.
- [x] Add optional pipeline hooks to run `scip print`/`snapshot`/`test`.
- [x] Capture tool versions (`scip-python --version`, `scip --version`) in diagnostics.

## Workstream 8: Incremental indexing + shard reuse
Goal: use scip-python shard artifacts to avoid full re-indexing for large repos.

Target files
- `src/extract/scip_indexer.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `docs/python_library_reference/scip_python_overview.md`

Representative code patterns
```python
if config.use_incremental_shards:
    options.extra_args = list(options.extra_args) + [
        "--index-shards",
        str(shards_dir),
    ]
```

Implementation checklist
- [x] Add config flags for shard reuse (shards dir, manifest path).
- [x] Wire shard options into `build_scip_index_options`.
- [x] Add diagnostics for shard hit/miss counts.

## Workstream 9: DataFusion view validation + schema introspection for SCIP
Goal: give SCIP views the same validation coverage as AST/CST/bytecode views.

Target files
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_introspection.py`

Representative code patterns
```python
def validate_scip_views(ctx: SessionContext) -> None:
    errors: dict[str, str] = {}
    _validate_required_functions(ctx, required=SCIP_REQUIRED_FUNCTIONS, errors=errors)
    for name in SCIP_VIEW_NAMES:
        ctx.sql(f"DESCRIBE SELECT * FROM {name}").collect()
    if errors:
        raise ValueError(f"SCIP view validation failed: {errors}.")
```

Implementation checklist
- [x] Define `SCIP_VIEW_NAMES` and `SCIP_REQUIRED_FUNCTIONS`.
- [x] Add view validation in runtime diagnostics (parallel to CST/bytecode).
- [x] Add information_schema checks for new columns (metadata, doc symbols, roles).

## Workstream 10: DataFusion registration + schema evolution for persisted outputs
Goal: register persisted scip outputs with explicit schemas and support schema evolution.

Target files
- `src/datafusion_engine/registry_loader.py`
- `src/datafusion_engine/listing_table_provider.py`
- `src/datafusion_engine/runtime.py`

Representative code patterns
```python
ctx.register_listing_table(
    name="scip_occurrences_v1",
    path=scip_occurrences_path,
    schema=scip_occurrences_schema,
    file_sort_order=[("path", "ascending"), ("start_line", "ascending")],
)
```

Implementation checklist
- [ ] Add pipeline wiring that populates `scip_dataset_locations` for persisted outputs.
- [x] Enable schema evolution adapter for scip outputs when versions change.
- [ ] Record information_schema snapshots after registration for audits.

## Cross-cutting tests
- [x] Unit tests for updated SCIP schemas (document linkage + role columns).
- [ ] Integration tests for scip metadata view columns and role decoding.
- [ ] Update `tests/fixtures/rule_signatures.json` once rule signature generation runs cleanly.
- [ ] Optional golden tests for `scip snapshot` output stability.
