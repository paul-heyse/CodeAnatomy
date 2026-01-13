## Hamilton Pipeline Compatibility Plan

### Goals
- Make optional extractors (tree-sitter/runtime inspect) fully Hamilton-orchestrated and overridable.
- Provide explicit, manual cache-busting controls without automatic repo watching.
- Reduce payload pressure for parallel execution by loading file contents on demand.
- Enable Hamilton-native caching/materialization for Arrow tables.
- Preserve existing schemas/contracts while improving configurability and observability.

### Constraints
- Preserve current dataset schemas and contract names unless a scope explicitly changes them.
- Do not introduce non-Arrow runtime dependencies beyond existing tree-sitter usage.
- Maintain strict typing and Ruff compliance with no suppressions.
- Keep default behavior identical (tree-sitter/runtime inspect off; caching opt-in).

---

### Scope 1: Hamilton-driven optional extraction toggles
**Description**
Replace the build-time `@config.when(...)` gating for tree-sitter/runtime inspect with
runtime input gating so the Hamilton instance can enable or disable these extractors
via `execute(overrides=...)`. This aligns optional extractors with the rest of the
input-driven pipeline and avoids hidden config-only behavior.

**Code pattern**
```python
# src/hamilton_pipeline/modules/extraction.py

def tree_sitter_bundle(
    enable_tree_sitter: bool,
    repo_root: str,
    repo_files: TableLike,
    file_contexts: Sequence[FileContext],
    ctx: ExecutionContext,
) -> Mapping[str, TableLike]:
    if not enable_tree_sitter:
        return {
            "ts_nodes": _empty_table(TS_NODES_SCHEMA),
            "ts_errors": _empty_table(TS_ERRORS_SCHEMA),
            "ts_missing": _empty_table(TS_MISSING_SCHEMA),
        }
    return extract_ts_tables(
        repo_files=repo_files,
        file_contexts=file_contexts,
        ctx=ctx,
    )
```

**Target files**
- Update: `src/hamilton_pipeline/modules/extraction.py`
- Update: `src/hamilton_pipeline/modules/inputs.py`

**Implementation checklist**
- [ ] Replace `tree_sitter_bundle` + `tree_sitter_bundle_disabled` with a single gated node.
- [ ] Replace `runtime_inspect_bundle` + `runtime_inspect_bundle_disabled` with a single gated node.
- [ ] Keep default input nodes (`enable_tree_sitter`, `enable_runtime_inspect`) returning False.
- [ ] Update docstrings to reflect that overrides enable optional extraction.

**Status**
Not started.

---

### Scope 2: Payload-on-demand file contexts and extractor options
**Description**
Add input-driven extract options and enable payload-on-demand for file contexts. The goal
is to allow lighter `repo_files` tables (no bytes/text) while extractors can still load
content from disk when needed. This reduces serialization overhead for parallel adapters
and keeps extraction configurable through Hamilton inputs.

**Code pattern**
```python
# src/hamilton_pipeline/modules/inputs.py

def repo_include_text() -> bool:
    return True


def repo_include_bytes() -> bool:
    return True


# src/extract/common.py
from pathlib import Path

def bytes_from_file_ctx(file_ctx: FileContext) -> bytes | None:
    if file_ctx.data is not None:
        return file_ctx.data
    if file_ctx.abs_path:
        return Path(file_ctx.abs_path).read_bytes()
    return None
```

**Target files**
- Update: `src/hamilton_pipeline/modules/inputs.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`
- Update: `src/extract/common.py`
- Update: `src/extract/repo_scan.py`
- Update: `src/extract/file_context.py`

**Implementation checklist**
- [ ] Add input nodes for payload policy (`repo_include_text`, `repo_include_bytes`).
- [ ] Pass payload flags into `RepoScanOptions` in `repo_files`.
- [ ] Extend `bytes_from_file_ctx` / `text_from_file_ctx` to read from disk when payload is missing.
- [ ] Verify extractors behave correctly when file payloads are omitted.

**Status**
Not started.

---

### Scope 3: Manual cache-busting controls for repo-dependent nodes
**Description**
Add a `cache_salt` input that can be set by the caller to manually invalidate cached
results for repo-dependent nodes. This keeps caching predictable without attempting
any automatic repo watching.

**Code pattern**
```python
# src/hamilton_pipeline/modules/inputs.py

def cache_salt() -> str:
    return ""


# src/hamilton_pipeline/modules/extraction.py

def repo_files(
    repo_scan_config: RepoScanConfig,
    cache_salt: str,
    ctx: ExecutionContext,
) -> TableLike:
    _ = cache_salt
    ...
```

**Target files**
- Update: `src/hamilton_pipeline/modules/inputs.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`

**Implementation checklist**
- [ ] Add `cache_salt` input node with a default empty string.
- [ ] Thread `cache_salt` into repo-dependent cached nodes (`repo_files`, `file_contexts`, `scip_index_path`).
- [ ] Document that changing `cache_salt` forces recomputation of cached nodes.

**Status**
Not started.

---

### Scope 4: Hamilton Arrow table caching/materialization adapters
**Description**
Provide a Hamilton data adapter for `pyarrow.Table` so cache/materialization can use
Parquet instead of pickle. This ensures large table caching is stable, inspectable, and
compatible with downstream toolchains.

**Code pattern**
```python
# src/hamilton_pipeline/arrow_adapters.py
from hamilton.io import data_adapters

class ArrowParquetAdapter(data_adapters.DataAdapter):
    def name(self) -> str:
        return "pyarrow_table_parquet"

    def can_load(self, type_: type) -> bool:
        return type_.__module__.startswith("pyarrow")

    def load_data(self, type_: type, **kwargs: object) -> object:
        ...

    def can_save(self, type_: type) -> bool:
        return type_.__module__.startswith("pyarrow")

    def save_data(self, data: object, **kwargs: object) -> dict[str, object]:
        ...
```

**Target files**
- New: `src/hamilton_pipeline/arrow_adapters.py`
- Update: `src/hamilton_pipeline/driver_factory.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [ ] Implement a `pyarrow.Table` adapter that reads/writes Parquet via `storage.parquet` helpers.
- [ ] Register the adapter in `build_driver` when caching is enabled.
- [ ] Switch heavy table nodes to `@cache(format="parquet")` where appropriate.
- [ ] Keep default cache behavior unchanged if the adapter is not configured.

**Status**
Not started.

---

### Scope 5: Inference-first schemas for Hamilton outputs
**Description**
Favor schema inference derived from inputs and data relationships. Only fall back to
registry contracts when inference has no evidence (e.g., empty outputs). After inference,
align outputs to the unified schema to keep ordering and types stable.

**Code pattern**
```python
# src/normalize/schema_infer.py

def infer_schema_or_registry(
    name: str,
    tables: Sequence[TableLike],
    *,
    opts: SchemaInferOptions | None = None,
) -> SchemaLike:
    if tables:
        return infer_schema_from_tables(tables, opts=opts)
    spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(name)
    if spec is not None:
        return spec.table_spec.to_arrow_schema()
    return pa.schema([])


# src/hamilton_pipeline/modules/normalization.py

def callsite_qname_candidates(...) -> TableLike:
    ...
    inferred = infer_schema_or_registry(
        "callsite_qname_candidates",
        [cst_callsites],
    )
    out = pa.Table.from_pylist(rows)
    return align_table_to_schema(out, inferred)
```

**Target files**
- Update: `src/normalize/schema_infer.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [ ] Add `infer_schema_or_registry(...)` helper for inference with registry fallback.
- [ ] Align inferred outputs to the unified schema for deterministic ordering/types.
- [ ] Use inference-first flow for row-based tables in normalization outputs.
- [ ] Preserve schema/contract versions unless explicitly revised.

**Status**
Not started.
