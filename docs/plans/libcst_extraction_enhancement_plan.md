## LibCST Extraction Enhancement Plan (SessionContext-First)

This plan integrates the full set of LibCST capabilities reviewed in:
- `docs/python_library_reference/libcst.md`
- `docs/python_library_reference/libcst-advanced.md`

It now treats **DataFusion SessionContext** as the canonical registry for schema,
catalogs, and view surfaces, aligning LibCST extraction with DataFusion’s
catalog/schema/table hierarchy and nested-schema querying model.

Each scope item includes the target pattern, file touch list, and implementation
checklist.

---

### Schema baseline: declare LibCST schemas in `schema_registry.py`

**Objective:** Treat the LibCST bundle schema as **declared** (not derived).
`datafusion_engine/schema_registry.py` is the source of truth for
`LIBCST_FILES_SCHEMA` and its nested `CST_*_T` struct types; extraction must
conform to that declared schema when registering tables in SessionContext.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa


CST_NODE_T = pa.struct(
    [
        ("cst_id", pa.int64()),
        ("kind", pa.string()),
        ("span", SPAN_T),
        ("span_ws", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

LIBCST_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("nodes", pa.list_(CST_NODE_T)),
        ("edges", pa.list_(CST_EDGE_T)),
        ("parse_manifest", pa.list_(CST_PARSE_MANIFEST_T)),
        ("parse_errors", pa.list_(CST_PARSE_ERROR_T)),
    ("refs", pa.list_(CST_REF_T)),
        ("imports", pa.list_(CST_IMPORT_T)),
        ("callsites", pa.list_(CST_CALLSITE_T)),
        ("defs", pa.list_(CST_DEF_T)),
        ("type_exprs", pa.list_(CST_TYPE_EXPR_T)),
    ("docstrings", pa.list_(CST_DOCSTRING_T)),
    ("decorators", pa.list_(CST_DECORATOR_T)),
    ("call_args", pa.list_(CST_CALL_ARG_T)),
        ("attrs", ATTRS_T),
    ]
)
```

**Target files**
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/extract_registry.py`

**Implementation checklist**
- [x] Keep `LIBCST_FILES_SCHEMA` as the declared contract for LibCST extraction.
- [x] Ensure all LibCST extraction tables registered in SessionContext match the
      declared schema (no runtime inference).
- [x] Extend `NESTED_DATASET_INDEX` to expose new LibCST outputs as views.

---

### Scope 1: SessionContext-first registration and view surfaces

**Objective:** Register LibCST extraction outputs directly into `SessionContext`
as nested tables, then expose exploded views for SQL consumers via DataFusion
`unnest`/`get_field` patterns.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa
from datafusion import SessionConfig, SessionContext


cfg = (
    SessionConfig()
    .with_create_default_catalog_and_schema(True)
    .with_default_catalog_and_schema("cpg", "public")
    .with_information_schema(True)
)
ctx = SessionContext(cfg)

batches = libcst_files_table.to_batches()
ctx.register_record_batches("libcst_files_v1", [batches])
ctx.sql(
    """
    CREATE OR REPLACE VIEW cst_callsites AS
    SELECT file_id, path, n
    FROM libcst_files_v1
    CROSS JOIN unnest(nodes) AS n
    WHERE n['kind'] = 'libcst.Call'
    """
)
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/extract_registry.py`
- `src/datafusion_engine/extract_builders.py`
- `src/datafusion_engine/extract_metadata.py`
- `src/datafusion_engine/extract_bundles.py`
- `src/datafusion_engine/listing_table_provider.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/hamilton_pipeline/modules/extraction.py`
- `src/hamilton_pipeline/modules/normalization.py`

**Implementation checklist**
- [x] Configure `SessionConfig` defaults (catalog/schema, `information_schema`,
      `show_schema`, view-type knobs) for LibCST registration sessions.
- [x] Register nested bundle tables via `register_record_batches` (in-memory)
      or `TableProviderCapsule` for Parquet listing providers.
- [x] Register LibCST tables using the declared schema from
      `schema_registry.schema_for("libcst_files_v1")` (no schema inference).
- [x] Create SQL views that explode `nodes/edges` using `unnest` and `get_field`.
- [x] Wire `datafusion_engine.extract_registry.dataset_schema()` to rely on
      `ctx.table(name).schema()` for canonical schema lookup.
- [x] Add an information-schema verification step (`SHOW COLUMNS` or
      `information_schema.columns`) for LibCST views.

---

### Scope 2: Repo-wide metadata + fully qualified names (FQNs)

**Objective:** Normalize symbol identities across modules via
`FullRepoManager` + `FullyQualifiedNameProvider`, emit stable FQNs, and make
them queryable as nested list fields in DataFusion.

**Pattern snippet**
```python
from __future__ import annotations

from collections.abc import Iterable

import libcst as cst
from libcst.metadata import FullRepoManager, FullyQualifiedNameProvider, MetadataWrapper


def metadata_wrapper_for_path(repo_root: str, paths: Iterable[str], path: str) -> MetadataWrapper:
    manager = FullRepoManager(
        repo_root_dir=repo_root,
        paths=tuple(paths),
        providers={FullyQualifiedNameProvider},
        use_pyproject_toml=True,
    )
    return manager.get_metadata_wrapper_for_path(path)


def resolve_fqns(wrapper: MetadataWrapper, node: cst.CSTNode) -> list[str]:
    fqn_map = wrapper.resolve(FullyQualifiedNameProvider)
    qset = fqn_map.get(node) or ()
    return sorted({q.name for q in qset})
```

**Target files**
- `src/extract/cst_extract.py`
- `src/datafusion_engine/extract_metadata.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/normalize/ibis_plan_builders.py`
- `src/normalize/ibis_api.py`
- `src/hamilton_pipeline/modules/normalization.py`

**Implementation checklist**
- [x] Add `compute_fully_qualified_names` + repo root inputs to `CSTExtractOptions`.
- [x] Initialize a `FullRepoManager` once per extraction run (cache per repo).
- [x] Emit `fqn` lists on `cst_defs` and `cst_callsites` rows.
- [x] Expose `fqn` lists in DataFusion views via `unnest` for relational joins.
- [x] Prefer FQNs when building `dim_qualified_names` and callsite candidates.

---

### Scope 3: Parse manifest + parse error enrichment

**Objective:** Capture LibCST parse fidelity and diagnostics in the parse
manifest and error tables, then surface them as DataFusion views in the session.

**Pattern snippet**
```python
from __future__ import annotations

import libcst as cst


def parse_manifest_row(module: cst.Module) -> dict[str, object]:
    return {
        "encoding": module.encoding,
        "default_indent": module.default_indent,
        "default_newline": module.default_newline,
        "has_trailing_newline": module.has_trailing_newline,
        "future_imports": list(module.future_imports),
        "libcst_version": getattr(cst, "__version__", None),
        "parser_backend": "native" if _has_native_parser() else "python",
        "parsed_python_version": getattr(module, "parsed_python_version", None),
    }
```

**Target files**
- `src/extract/cst_extract.py`
- `src/datafusion_engine/extract_metadata.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/normalize/catalog.py`

**Implementation checklist**
- [x] Extend `cst_parse_manifest` schema with LibCST version + backend fields.
- [x] Extend `cst_parse_errors` schema with editor position + context fields.
- [x] Emit enriched rows in `_parse_module` / `_manifest_row`.
- [x] Update DataFusion SQL fragments to expose new columns.
- [x] Surface manifest/error views via SessionContext for diagnostics pipelines.

---

### Scope 4: Scope-aware references + attribute coverage

**Objective:** Use `ScopeProvider` + `ParentNodeProvider` to classify references,
capture attribute/dotted references, and expose a unified `cst_refs` view in
DataFusion.

**Pattern snippet**
```python
from __future__ import annotations

import libcst as cst
from libcst import helpers
from libcst.metadata import ParentNodeProvider, ScopeProvider


def reference_row(node: cst.CSTNode, scope_map, parent_map) -> dict[str, object]:
    parent = parent_map.get(node)
    scope = scope_map.get(node)
    dotted = helpers.get_full_name_for_node(node) if isinstance(node, cst.Attribute) else None
    return {
        "ref_kind": "attribute" if isinstance(node, cst.Attribute) else "name",
        "ref_text": dotted or getattr(node, "value", None),
        "parent_kind": type(parent).__name__ if parent else None,
        "scope_type": getattr(scope, "scope_type", None) if scope else None,
    }
```

**Target files**
- `src/extract/cst_extract.py`
- `src/datafusion_engine/extract_metadata.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/relspec/rules/relationship_specs.py`
- `src/relspec/cpg/build_nodes.py`
- `src/relspec/cpg/build_props.py`

**Implementation checklist**
- [x] Add `ScopeProvider` + `ParentNodeProvider` to metadata resolution.
- [x] Replace `cst_name_refs` with `cst_refs` to capture
      both `Name` and `Attribute` references.
- [x] Store scope metadata (scope id/type, ref role, parent kind).
- [x] Update relationship rules to prefer `cst_refs` when present.
- [x] Expose `cst_refs` via a DataFusion view for downstream joins.

---

### Scope 5: Docstrings + decorators + definition metadata

**Objective:** Enrich definition rows with docstrings and decorators, and emit
dedicated datasets that can be joined via DataFusion SessionContext views.

**Pattern snippet**
```python
from __future__ import annotations

import libcst as cst
from libcst import helpers


def def_docstring(node: cst.FunctionDef | cst.ClassDef | cst.Module) -> str | None:
    return node.get_docstring(clean=True)


def decorator_rows(node: cst.FunctionDef | cst.ClassDef) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for decorator in node.decorators:
        expr = decorator.decorator
        rows.append({"decorator_text": helpers.get_full_name_for_node(expr)})
    return rows
```

**Target files**
- `src/extract/cst_extract.py`
- `src/datafusion_engine/extract_metadata.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/normalize/registry_templates.py`

**Implementation checklist**
- [x] Add `cst_docstrings` dataset (module/class/function docstrings + spans).
- [x] Add `cst_decorators` dataset (owner id + decorator text + spans).
- [x] Attach docstring/decorator fields to `cst_defs` rows when present.
- [x] Add SQL fragments for `cst_docstrings` and `cst_decorators`.
- [x] Register views for docstrings/decorators in SessionContext.

---

### Scope 6: Callsite argument structure

**Objective:** Capture call argument shapes (positional/keyword/star), enabling
querying for call patterns beyond `arg_count` via DataFusion views.

**Pattern snippet**
```python
from __future__ import annotations

import libcst as cst


def call_arg_rows(call_id: str, node: cst.Call) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx, arg in enumerate(node.args):
        rows.append(
            {
                "call_id": call_id,
                "arg_index": idx,
                "keyword": arg.keyword.value if arg.keyword else None,
                "star": arg.star,
            }
        )
    return rows
```

**Target files**
- `src/extract/cst_extract.py`
- `src/datafusion_engine/extract_metadata.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/schema_registry.py`
- `src/normalize/ibis_plan_builders.py`

**Implementation checklist**
- [x] Add `cst_call_args` dataset (call id + arg shape + spans + value text).
- [x] Emit `call_id` consistently for callsite/arg joins.
- [x] Expose `cst_call_args` via DataFusion views for arg-level analysis.
- [x] Extend callsite normalization to use arg-level metadata when present.

---

### Scope 7: Optional type inference integration (Pyre-backed)

**Objective:** Attach inferred types to references/callsites when Pyre is
available, storing them as queryable fields in DataFusion.

**Pattern snippet**
```python
from __future__ import annotations

from libcst.metadata import TypeInferenceProvider


def inferred_type_map(wrapper):
    return wrapper.resolve(TypeInferenceProvider)
```

**Target files**
- `src/extract/cst_extract.py`
- `src/datafusion_engine/extract_templates.py`
- `src/datafusion_engine/extract_metadata.py`
- `src/datafusion_engine/schema_registry.py`

**Implementation checklist**
- [x] Add `compute_type_inference` option and gate Pyre wiring.
- [x] Emit `inferred_type` fields on `cst_refs` and `cst_callsites` rows.
- [x] Add dataset metadata describing inference availability.
- [x] Ensure extraction cleanly degrades when Pyre is absent.

---

### Scope 8: SessionConfig hardening + schema evolution adapters

**Objective:** Align LibCST extraction with DataFusion schema hardening and
evolution hooks (`PhysicalExprAdapterFactory`, view-type behavior, listing-table
schema contracts).

**Pattern snippet**
```python
from __future__ import annotations

from datafusion import SessionContext


ctx = SessionContext()
ctx.sql("SET datafusion.catalog.information_schema = true")
ctx.sql("SET datafusion.explain.show_schema = true")
ctx.sql("SET datafusion.sql_parser.map_string_types_to_utf8view = false")
ctx.sql("SET datafusion.execution.parquet.schema_force_view_types = false")
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/listing_table_provider.py`
- `src/datafusion_engine/schema_registry.py`
- `rust/datafusion_ext/src/lib.rs`
- `src/obs/manifest.py`

**Implementation checklist**
- [x] Pin session config defaults for `information_schema`, `show_schema`, and
      view-type coercions.
- [x] Route Parquet listing providers through schema IPC to enforce canonical
      nested schemas on registration.
- [x] Integrate `PhysicalExprAdapterFactory` installation for schema evolution
      at scan boundaries when available.
- [x] Emit schema fingerprints into extraction manifests for drift detection.

---

### Scope 9: Schema + downstream usage alignment

**Objective:** Update dataset metadata, bundle wiring, and downstream consumers
to align with new LibCST outputs and SessionContext-based schemas.

**Pattern snippet**
```python
from __future__ import annotations

from datafusion_engine.extract_metadata import ExtractMetadata


CST_DOCSTRINGS = ExtractMetadata(
    name="cst_docstrings_v1",
    version=1,
    bundles=("cst_bundle",),
    fields=("file_id", "path", "owner_kind", "docstring", "bstart", "bend"),
    template="cst",
)
```

**Target files**
- `src/datafusion_engine/extract_metadata.py`
- `src/datafusion_engine/extract_templates.py`
- `src/datafusion_engine/extract_bundles.py`
- `src/datafusion_engine/extract_builders.py`
- `src/datafusion_engine/extract_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/normalize/ibis_api.py`
- `src/normalize/ibis_plan_builders.py`
- `src/relspec/rules/relationship_specs.py`
- `src/relspec/cpg/build_nodes.py`
- `src/relspec/cpg/build_props.py`

**Implementation checklist**
- [x] Update `datafusion_engine/schema_registry.py` with declared schema changes
      listed below (no derived/inferred LibCST schema).
- [x] Register nested `cst_*` datasets via `NESTED_DATASET_INDEX` + fragment views
      (no separate extract catalog entries needed).
- [x] Keep `cst_bundle` scoped to `libcst_files_v1` (nested views are derived).
- [x] Update DataFusion fragment SQL and view specs for new datasets.
- [x] Update normalization and relspec inputs to incorporate new columns/tables.
- [ ] Add/update tests validating schema presence and view outputs.

**Schema updates required for new outputs**
- **`CST_PARSE_MANIFEST_T`**: add `libcst_version`, `parser_backend`,
  `parsed_python_version`, `schema_fingerprint`.
- **`CST_PARSE_ERROR_T`**: add `editor_line`, `editor_column`, `context`.
- **`CST_CALLSITE_T`**:
  - add `call_id` for stable joins,
  - add `callee_fqns: List<Utf8>` for fully-qualified call targets,
  - add `inferred_type` (optional, nullable) when Pyre is enabled.
- **`CST_DEF_T`**:
  - add `def_id` (stable join key),
  - add `def_fqns: List<Utf8>`,
  - add `docstring` (nullable) and `decorator_count` (optional).
- **New nested structs** in `LIBCST_FILES_SCHEMA`:
  - `CST_REF_T` (scope-aware refs: `ref_id`, `ref_kind`, `ref_text`, `scope_type`,
    `scope_name`, `scope_role`, `parent_kind`, `inferred_type`),
  - `CST_DOCSTRING_T` (owner kind/id + docstring + spans),
  - `CST_DECORATOR_T` (owner kind/id + decorator text + spans),
  - `CST_CALL_ARG_T` (call_id + arg_index + keyword + star + arg spans/text).
- **`LIBCST_FILES_SCHEMA` additions**:
  - add list fields: `refs`, `docstrings`, `decorators`, `call_args`
    (`refs` replaces `name_refs`; compatibility shims removed).
- **`NESTED_DATASET_INDEX`**:
  - add `cst_refs`, `cst_docstrings`, `cst_decorators`, `cst_call_args` pointing
    to the new list fields in `libcst_files_v1`.
