# Semantic Catalog

This document defines the semantic catalog as the single source of truth for
dataset definitions, schemas, contracts, and view builders across the semantic
pipeline. It replaces all legacy `normalize/*` registries.

## Purpose

The semantic catalog centralizes:

- **Dataset rows** (names, fields, merge keys, metadata)
- **Dataset specs** (schema contracts + metadata)
- **View builders** (semantic normalizations and analysis/diagnostic builders)
- **Aliases and naming policy** (canonical output names)

The intent is to keep dataset definitions, schema validation, and view planning
consistent across the compiler, runtime, and DataFusion planning layers.

## Canonical Modules

- `src/semantics/catalog/dataset_rows.py`
  - Defines `SemanticDatasetRow` and the authoritative dataset rows.
  - Includes semantic, analysis, and diagnostic outputs.
- `src/semantics/catalog/dataset_specs.py`
  - Builds `DatasetSpec` entries from dataset rows.
  - Provides `dataset_spec`, `dataset_schema`, and `dataset_name_from_alias`.
- `src/semantics/catalog/spec_builder.py`
  - Constructs schema contracts and metadata for dataset rows.
- `src/semantics/catalog/view_builders.py`
  - Registry for semantic normalization + relationship builders.
- `src/semantics/catalog/analysis_builders.py`
  - DataFusion builders for analysis/diagnostic outputs (CFG, def-use, diagnostics).

## Data Model

1. **Dataset rows** define the structure (fields, join keys, merge keys, metadata).
2. **Dataset specs** derive Arrow schemas and contracts from the rows.
3. **View builders** materialize DataFusion plans for each dataset row.
4. **Runtime** uses the catalog to resolve dataset locations and schema contracts.

## Adding a New Dataset

1. Add a `SemanticDatasetRow` entry in `dataset_rows.py`.
2. Update or add a corresponding view builder in `view_builders.py` or
   `analysis_builders.py`.
3. Ensure the dataset has:
   - A canonical name with `_v1` suffix.
   - Correct `join_keys` and `merge_keys`.
   - Appropriate `supports_cdf` and `partition_cols`.
4. Confirm `dataset_spec(...)` resolves and that schema metadata is correct.

## Output Location Resolution

Runtime dataset locations are derived from the catalog:

- `normalize_output_root` and `semantic_output_root` in `DataFusionRuntimeProfile`
  are expanded into `DatasetLocation` entries for all semantic datasets.
- `semantic_output_catalog_name` can be used to pin an explicit output catalog.

## Invariants

- The semantic catalog is the **only** registry for dataset specs and view builders.
- All output schemas must be derivable from `SemanticDatasetRow` entries.
- View registration uses catalog builders, not ad-hoc SQL registries.

