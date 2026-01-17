Incremental Symbol Closure Execution Plan
========================================

This plan implements symbol-aware incremental impact analysis on top of the existing state store,
upsert pipeline, and SCIP snapshot diffing. It synthesizes the goals in
`docs/plans/incremental_updates_plan.md` and `docs/plans/additional_incremental_updates.md` into a
sequenced execution plan.

Goals
-----
- Shrink incremental work by computing impacted file_ids via export deltas plus callsite and
  reverse-import closure.
- Preserve correctness: incremental outputs must match a full run for the same repo state.
- Provide diagnostics that explain why each file was rebuilt.

Non-goals
---------
- Do not change the external CLI or run bundle layout beyond adding new diagnostic datasets.
- Do not remove the existing import-closure path until symbol-closure is proven stable.

Phase 1 - Impact plumbing
-------------------------

Scope 1.1 - Add an impact data model and gate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create a dedicated impact model so we can evolve beyond changed-only diffs without breaking the
existing `IncrementalFileChanges` API.

Code pattern
```python
from dataclasses import dataclass

@dataclass(frozen=True)
class IncrementalImpact:
    changed_file_ids: tuple[str, ...] = ()
    deleted_file_ids: tuple[str, ...] = ()
    impacted_file_ids: tuple[str, ...] = ()
    full_refresh: bool = False
```

Target files
- `src/incremental/types.py`
- `src/hamilton_pipeline/modules/incremental.py`

Implementation checklist
- [x] Add `IncrementalImpact` dataclass with clear semantics and defaults.
- [x] Add a Hamilton node `incremental_impact(...) -> IncrementalImpact` that:
  - starts with `IncrementalFileChanges`
  - merges in SCIP-changed file_ids if enabled
  - produces `impacted_file_ids` (initially equal to `changed_file_ids`).
- [x] Update exports in `src/incremental/__init__.py`.

Scope 1.2 - Use impacted file_ids for extraction and relspec scoping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
All file-scoped work must pivot to `impacted_file_ids` rather than `changed_file_ids`.

Code pattern
```python
def repo_files_extract(
    repo_files: TableLike,
    incremental_config: IncrementalConfig,
    incremental_impact: IncrementalImpact,
) -> TableLike:
    if not incremental_config.enabled:
        return repo_files
    return _filter_repo_files_by_ids(repo_files, incremental_impact.impacted_file_ids)
```

Target files
- `src/hamilton_pipeline/modules/extraction.py`
- `src/incremental/relspec_update.py`
- `src/hamilton_pipeline/modules/cpg_build.py`

Implementation checklist
- [x] Use `incremental_impact.impacted_file_ids` for relspec scoping (`relspec_inputs_from_state`).
- [x] Switch extract scoping (`repo_files_extract`) to use
  `incremental_extract_impact.impacted_file_ids` (impact seed from repo diffs to avoid cycles).
- [x] Keep the existing file-change model for delete handling and state-store cleanup.
- [x] Add wiring from `incremental_impact` to relspec scoping helpers.

Phase 2 - Import resolution foundation
--------------------------------------

Scope 2.1 - Module index dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Introduce a deterministic `module_name_from_path` and per-file module index.

Code pattern
```python
def module_name_from_path(path: str) -> str | None:
    # src/pkg/__init__.py -> pkg
    # src/pkg/foo.py -> pkg.foo
    ...

def build_module_index(repo_files: pa.Table) -> pa.Table:
    return pa.table(
        {
            "file_id": repo_files["file_id"],
            "path": repo_files["path"],
            "module_fqn": module_names,
            "is_init": is_init_flags,
        }
    )
```

Target files
- `src/incremental/module_index.py`
- `src/incremental/index_update.py`
- `src/incremental/registry_rows.py`
- `src/hamilton_pipeline/modules/incremental.py`

Implementation checklist
- [x] Add `py_module_index_v1` dataset spec (file_id, path, module_fqn, is_init).
- [x] Implement module index builder with strict, testable path rules.
- [x] Upsert into `state_store.dataset_dir("py_module_index_v1")`.

Scope 2.2 - Imports resolved dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Resolve `cst_imports_norm` to absolute module + imported name to enable reverse import closure.

Code pattern
```python
def resolve_imports(
    cst_imports_norm: TableLike,
    module_index: TableLike,
) -> TableLike:
    # map relative imports to module_fqn using importer module
    # emit: importer_file_id, importer_path, imported_module_fqn, imported_name, is_star
    ...
```

Target files
- `src/incremental/imports_resolved.py`
- `src/incremental/index_update.py`
- `src/incremental/registry_rows.py`
- `src/hamilton_pipeline/modules/incremental.py`

Implementation checklist
- [x] Add `py_imports_resolved_v1` dataset spec with file_id partitioning.
- [x] Implement resolution with a clear rule for relative levels.
- [x] Upsert into the state store for incremental runs.

Phase 3 - Symbol-level artifacts (export delta foundations)
-----------------------------------------------------------

Scope 3.1 - rel_def_symbol relationship rule and contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Align top-level defs to SCIP symbols to enrich exported definitions.

Code pattern
```python
_interval_align_rule(
    _IntervalAlignRuleSpec(
        name="cst_defs__name_to__scip_occurrences",
        output_dataset="rel_def_symbol",
        contract_name="rel_def_symbol_v1",
        inputs=("cst_defs", "scip_occurrences"),
        left_start_col="bstart",
        left_end_col="bend",
        select_left=("def_id", "path", "bstart", "bend"),
    )
)
```

Target files
- `src/relspec/rules/relationship_specs.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/incremental/relspec_update.py`

Implementation checklist
- [x] Add `rel_def_symbol_v1` dataset and contract specs.
- [x] Extend `CstRelspecInputs` and relspec input wiring to include `cst_defs_norm`.
- [x] Register and persist `rel_def_symbol` in the relationship outputs.

Scope 3.2 - Exported defs index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create the per-file exported definition index to enable cheap export deltas.

Code pattern
```python
def build_exported_defs_index(
    cst_defs_norm: TableLike,
    *,
    ctx: ExecutionContext,
    rel_def_symbol: TableLike | None = None,
) -> TableLike:
    top = cst_defs_norm.filter(pc.is_null(cst_defs_norm["container_def_id"]))
    qnames = pc.list_flatten(top["qnames"])
    qname_ids = prefixed_hash_id([qnames], prefix="qname")
    ...
```

Target files
- `src/incremental/exports.py`
- `src/incremental/exports_update.py`
- `src/hamilton_pipeline/modules/incremental.py`
- `src/incremental/registry_rows.py`

Implementation checklist
- [x] Add `dim_exported_defs_v1` dataset spec with `file_id` partitioning.
- [x] Ensure qname hashing uses `prefix="qname"` to align with `dim_qualified_names`.
- [x] Optionally join to `rel_def_symbol` when SCIP is available.
- [x] Upsert into the state store on incremental runs.

Scope 3.3 - Export delta dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Diff prior vs current exported defs for changed files only.

Code pattern
```python
def compute_changed_exports(
    *,
    backend: BaseBackend,
    prev_exports: str,
    curr_exports: str,
    changed_files: TableLike,
) -> TableLike:
    prev = backend.read_parquet(prev_exports)
    curr = backend.read_parquet(curr_exports)
    ...
```

Target files
- `src/incremental/deltas.py`
- `src/hamilton_pipeline/modules/incremental.py`
- `src/incremental/registry_rows.py`

Implementation checklist
- [x] Add `inc_changed_exports_v1` dataset spec.
- [x] Ensure diff keys include `file_id` and `qname_id` (and `symbol` when present).
- [x] Store delta rows with `delta_kind` ("added" or "removed").

Phase 4 - Impact closure and strategy
-------------------------------------

Scope 4.1 - Reverse callsite closure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Use prior run relationships to map changed exports to callers.

Code pattern
```python
def impacted_callers_from_changed_exports(
    *,
    backend: BaseBackend,
    changed_exports: TableLike,
    prev_rel_callsite_qname: str,
    prev_rel_callsite_symbol: str | None,
) -> TableLike:
    qname_hits = rel_callsite_qname.join(changed, "qname_id")
    symbol_hits = rel_callsite_symbol.join(changed, "symbol")
    return qname_hits.union_all(symbol_hits).distinct()
```

Target files
- `src/incremental/impact.py`
- `src/incremental/impact_update.py`
- `src/hamilton_pipeline/modules/incremental.py`
- `src/incremental/registry_rows.py`

Implementation checklist
- [x] Add `inc_impacted_callers_v1` dataset spec (file_id, path, reason columns).
- [x] Use `edge_owner_file_id` to attribute a callsite to the caller file_id.
- [x] Distinct results and persist to the state store.

Scope 4.2 - Reverse import closure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Map changed exports to importers using the resolved imports dataset.

Code pattern
```python
def impacted_importers_from_changed_exports(
    *,
    backend: BaseBackend,
    changed_exports: TableLike,
    prev_imports_resolved: str,
) -> TableLike:
    # match from-imports and star-imports by module + name
    ...
```

Target files
- `src/incremental/impact.py`
- `src/incremental/impact_update.py`
- `src/hamilton_pipeline/modules/incremental.py`
- `src/incremental/registry_rows.py`

Implementation checklist
- [x] Add `inc_impacted_importers_v1` dataset spec.
- [x] Derive module/name from `qname` when not explicitly present.
- [x] Always include star importers for the changed export module.

Scope 4.3 - Impact strategy and final impacted set
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Provide strategy selection and produce the final impacted file set.

Code pattern
```python
def merge_impacted_files(
    changed: TableLike,
    callers: TableLike,
    importers: TableLike,
    *,
    strategy: str,
) -> TableLike:
    if strategy == "symbol_closure":
        return union_all(changed, callers, importers).distinct()
    if strategy == "import_closure":
        return import_closure_only(...)
    return union_all(changed, callers, importers, import_closure_only(...)).distinct()
```

Target files
- `src/incremental/types.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/incremental.py`
- `src/incremental/impact.py`
- `src/incremental/registry_rows.py`

Implementation checklist
- [x] Add `impact_strategy` to `IncrementalConfig` with default "hybrid".
- [x] Add `inc_impacted_files_v2` dataset spec and state store persistence.
- [x] Wire `incremental_impact` to use `inc_impacted_files_v2`.
- [x] Expose `incremental_impact_strategy` on execution/CLI entry points (optional if using
  `IncrementalConfig` overrides directly).

Phase 5 - Diagnostics and tests
-------------------------------

Scope 5.1 - Persist impact diagnostics to run bundles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Expose incremental impact datasets for run-level inspection.

Code pattern
```python
@tag(layer="obs", artifact="write_inc_impacted_files_parquet", kind="table")
def write_inc_impacted_files_parquet(
    output_dir: str,
    inc_impacted_files_v2: TableLike,
) -> str:
    ...
```

Target files
- `src/hamilton_pipeline/modules/outputs.py`
- `src/obs/repro.py`

Implementation checklist
- [x] Add write nodes for `inc_changed_exports`, `inc_impacted_callers`,
  `inc_impacted_importers`, and `inc_impacted_files_v2`.
- [x] Register them in the run bundle with stable paths under `build/e2e_full_pipeline`.

Scope 5.2 - Impact reasons schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Make the impacted-file diagnostics actionable with reason codes.

Code pattern
```python
reason_kind = pa.array(["changed_file", "caller_symbol", "import_star"], pa.string())
reason_ref = pa.array(["pkg.mod.foo", "pkg.mod.foo", "pkg.mod"], pa.string())
```

Target files
- `src/incremental/registry_rows.py`
- `src/incremental/impact.py`

Implementation checklist
- [x] Add `reason_kind` and `reason_ref` columns to impacted datasets.
- [x] Ensure all closures annotate the reason for inclusion.

Scope 5.3 - Unit and integration tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Add tests that pin the new behavior and prevent regressions.

Target files
- `tests/unit/test_incremental_exports.py`
- `tests/unit/test_incremental_export_delta.py`
- `tests/unit/test_incremental_call_closure.py`
- `tests/integration/test_incremental_symbol_closure_smoke.py`

Implementation checklist
- [x] Cover export index building and `qname_id` hashing.
- [x] Cover delta semantics (added/removed).
- [x] Cover reverse call and import closures.
- [x] Add a smoke integration that demonstrates reduced impacted set.

Definition of done
------------------
- [x] Impact strategy defaults to "hybrid" and can be flipped to "symbol_closure".
- [x] Incremental runs only rebuild impacted files derived from exports + callers + imports.
- [x] Run bundles include impact diagnostics with reasons and no schema violations.
- [x] Unit and integration tests cover export index, deltas, and closure logic.
