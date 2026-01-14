## Extract Configuration Defaults + Policy Expansion Plan

### Goals
- Apply consistent extractor defaults and enablement rules across all extract datasets.
- Normalize option hashing and metadata so outputs are reproducible with partial options.
- Centralize schema policy defaults at the template level with explicit dataset overrides.
- Prefer programmatic configuration over ad-hoc extractor branching.

### Constraints
- Preserve existing output schemas and column names unless explicitly extended.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep extractor logic focused on parsing/row emission; move configuration to registries.

---

### Scope 1: Expand ExtractorConfigSpec Coverage
**Description**
Define defaults and feature flags for every extractor so configuration is centralized
and consistent across datasets (including expanded CST/bytecode defaults).

**Code patterns**
```python
# src/extract/registry_templates.py
CONFIGS = {
    "ast": ExtractorConfigSpec(
        extractor_name="ast",
        defaults={
            "type_comments": True,
            "feature_version": None,
        },
    ),
    "repo_scan": ExtractorConfigSpec(
        extractor_name="repo_scan",
        defaults={
            "include_globs": ("**/*.py",),
            "exclude_dirs": (".git", "__pycache__", ".venv", "venv", "node_modules"),
            "exclude_globs": (),
            "follow_symlinks": False,
            "include_bytes": True,
            "include_text": True,
            "max_file_bytes": None,
            "max_files": None,
            "repo_id": None,
        },
    ),
    "scip": ExtractorConfigSpec(
        extractor_name="scip",
        defaults={
            "prefer_protobuf": True,
            "allow_json_fallback": False,
            "dictionary_encode_strings": True,
        },
    ),
}
```

**Target files**
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/registry_specs.py` (expose defaults accessor)

**Implementation checklist**
- [x] Add ExtractorConfigSpec entries for `repo_scan`, `ast`, `symtable`,
      `runtime_inspect`, and `scip`.
- [x] Include defaults for feature flags and non-flag options where reasonable.
- [x] Keep defaults aligned with existing dataclass defaults.

**Status**
Completed.

---

### Scope 2: Normalize Options for Hashing + Enablement
**Description**
Normalize extractor options by merging defaults with overrides before computing
options hashes and enablement checks.

**Code patterns**
```python
# src/extract/registry_specs.py
def normalize_options[T](name: str, options: object | None, factory: type[T]) -> T:
    defaults = extractor_defaults(name)
    if options is None:
        return factory(**defaults)
    merged = {**defaults, **_options_dict(options)}
    return factory(**merged)


# src/extract/ast_extract.py
normalized_options = normalize_options("ast", options, ASTExtractOptions)
nodes_meta = dataset_metadata_with_options("py_ast_nodes_v1", options=normalized_options)
```

**Target files**
- Add: `src/extract/registry_specs.py` (normalize_options helper)
- Update: `src/extract/repo_scan.py`
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/scip_extract.py`

**Implementation checklist**
- [x] Add normalize_options helper and usage examples in extractors.
- [x] Replace direct `options = options or ...` with normalization.
- [x] Ensure metadata hashing uses normalized options for stability.

**Status**
Completed.

---

### Scope 3: Template-Level Schema Policy Defaults
**Description**
Define baseline schema policies per extractor template and allow dataset overrides
to refine behavior.

**Code patterns**
```python
# src/extract/registry_policies.py
_TEMPLATE_POLICIES = {
    "repo_scan": TemplatePolicyRow(template="repo_scan", keep_extra_columns=True),
    "runtime_inspect": TemplatePolicyRow(template="runtime_inspect", keep_extra_columns=True),
    "scip": TemplatePolicyRow(template="scip", keep_extra_columns=False),
}


# src/extract/registry_specs.py
def dataset_schema_policy(...):
    row = policy_row(name)
    template_row = template_policy_row(dataset_row(name).template)
    keep_extra_columns = (
        row.keep_extra_columns if row and row.keep_extra_columns is not None else None
    )
    if keep_extra_columns is None and template_row is not None:
        keep_extra_columns = template_row.keep_extra_columns
    options = SchemaPolicyOptions(keep_extra_columns=keep_extra_columns, ...)
    return schema_policy_factory(spec, ctx=ctx, options=options)
```

**Target files**
- Update: `src/extract/registry_policies.py`
- Update: `src/extract/registry_specs.py`

**Implementation checklist**
- [x] Define per-template policy defaults.
- [x] Merge template defaults with dataset-specific overrides.
- [x] Ensure schema policy application is consistent across extractors.

**Status**
Completed.

---

### Scope 4: Additional Dataset Enablement Rules
**Description**
Add high-signal enablement rules beyond current CST/tree-sitter/bytecode flags.

**Code patterns**
```python
# src/extract/registry_rows.py
def _allowlist_enabled(options: object | None) -> bool:
    if options is None:
        return False
    allowlist = getattr(options, "module_allowlist", ())
    return bool(allowlist)
```

**Target files**
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/runtime_inspect_extract.py`

**Implementation checklist**
- [x] Gate runtime-inspect outputs on `module_allowlist`.
- [x] Review other extractors for similar high-signal enablement rules.

**Status**
Completed.
