## Extract Template Expansion + Error Artifacts Parquet Plan

### Goals
- Expand template-driven dataset families beyond tree-sitter/runtime-inspect.
- Keep extract registries table-driven while reducing duplicated row specs.
- Persist extract validation artifacts (errors/stats/alignment) to Parquet for auditability.

### Constraints
- Preserve output schemas, column names, and metadata semantics for extract tables.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep changes in `src/normalize` and `src/cpg` limited to planned scope only.

---

### Scope 1: Expand Template-Driven Dataset Families
**Description**
Move AST/CST/bytecode/symtable/SCIP/repo-scan dataset rows into template
expansions so registry tables are generated from dataset-family specs.

**Code patterns**
```python
# src/extract/registry_template_specs.py
DATASET_TEMPLATE_SPECS: tuple[DatasetTemplateSpec, ...] = (
    DatasetTemplateSpec(name="tree_sitter_core", template="tree_sitter"),
    DatasetTemplateSpec(name="runtime_inspect_core", template="runtime_inspect"),
    DatasetTemplateSpec(name="ast_core", template="ast"),
    DatasetTemplateSpec(name="cst_core", template="cst"),
    DatasetTemplateSpec(name="bytecode_core", template="bytecode"),
    DatasetTemplateSpec(name="symtable_core", template="symtable"),
    DatasetTemplateSpec(name="scip_core", template="scip"),
    DatasetTemplateSpec(name="repo_scan_core", template="repo_scan"),
)
```

```python
# src/extract/registry_templates.py
def _ast_records(_: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    return (
        {"name": "py_ast_nodes_v1", "version": 1, "template": "ast", ...},
        {"name": "py_ast_edges_v1", "version": 1, "template": "ast", ...},
        {"name": "py_ast_errors_v1", "version": 1, "template": "ast", ...},
    )


TEMPLATE_REGISTRY: dict[str, TemplateFn] = {
    "ast_core": _ast_records,
    "cst_core": _cst_records,
    "bytecode_core": _bytecode_records,
    "symtable_core": _symtable_records,
    "scip_core": _scip_records,
    "repo_scan_core": _repo_scan_records,
    "tree_sitter_core": _tree_sitter_records,
    "runtime_inspect_core": _runtime_inspect_records,
}
```

```python
# src/extract/registry_tables.py
TEMPLATE_ROW_RECORDS = expand_dataset_templates(DATASET_TEMPLATE_SPECS)
EXTRACT_DATASET_TABLE = extract_dataset_table_from_rows(
    (*DATASET_ROW_RECORDS, *TEMPLATE_ROW_RECORDS)
)
```

**Target files**
- Update: `src/extract/registry_template_specs.py`
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/registry_tables.py`

**Implementation checklist**
- [ ] Add template specs for AST/CST/bytecode/symtable/SCIP/repo-scan.
- [ ] Implement template expansion functions for each family.
- [ ] Remove per-family rows from `DATASET_ROW_RECORDS` once templates produce parity.
- [ ] Verify `DATASET_ROW_SPECS` parity (schema/fields/join_keys/postprocess).

**Status**
Planned.

---

### Scope 2: Parquet Writer for Extract Error Artifacts
**Description**
Persist extract validation artifacts (errors/stats/alignment) to Parquet using
the existing `ExtractErrorArtifacts` bundle.

**Code patterns**
```python
# src/hamilton_pipeline/modules/outputs.py
@tag(layer="materialize", artifact="extract_errors_parquet", kind="side_effect")
def write_extract_error_artifacts_parquet(
    output_dir: str | None,
    extract_error_artifacts: ExtractErrorArtifacts,
) -> JsonDict | None:
    if not output_dir:
        return None
    base = Path(output_dir) / "extract_errors"
    _ensure_dir(base)
    report: JsonDict = {"base_dir": str(base), "datasets": {}}
    for output, errors in extract_error_artifacts.errors.items():
        dataset_dir = base / output
        _ensure_dir(dataset_dir)
        report["datasets"][output] = {
            "errors": write_table_parquet(errors, dataset_dir / "errors.parquet"),
            "stats": write_table_parquet(
                extract_error_artifacts.stats[output],
                dataset_dir / "error_stats.parquet",
            ),
            "alignment": write_table_parquet(
                extract_error_artifacts.alignment[output],
                dataset_dir / "alignment.parquet",
            ),
        }
    return report
```

**Target files**
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/arrowdsl/io/parquet.py` (optional helper for error artifacts)

**Implementation checklist**
- [ ] Add a materializer node to write extract errors/stats/alignment per output.
- [ ] Use a stable path layout: `extract_errors/<output>/{errors,error_stats,alignment}.parquet`.
- [ ] Return a compact report with per-output paths and counts.
- [ ] Gate writes behind `output_dir` like other output materializers.

**Status**
Planned.
