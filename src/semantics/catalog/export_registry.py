"""Registry of exportable semantic datasets."""

from __future__ import annotations

from typing import Final

from semantics.catalog.dataset_rows import SEMANTIC_SCHEMA_VERSION, SemanticDatasetRow

EXPORT_DATASETS: Final[tuple[SemanticDatasetRow, ...]] = (
    SemanticDatasetRow(
        name="dim_exported_defs",
        version=SEMANTIC_SCHEMA_VERSION,
        bundles=(),
        fields=(
            "file_id",
            "path",
            "def_id",
            "def_kind_norm",
            "name",
            "qname_id",
            "qname",
            "qname_source",
            "symbol",
            "symbol_roles",
        ),
        category="analysis",
        supports_cdf=True,
        merge_keys=("file_id", "def_id", "qname_id"),
        join_keys=("file_id", "def_id", "qname_id"),
        template="exported_defs",
        view_builder="exported_defs_df_builder",
        register_view=True,
        source_dataset=None,
        entity="def",
        grain="per_def",
        stability="design",
        materialization="delta",
        materialized_name="semantic.dim_exported_defs",
    ),
)

__all__ = ["EXPORT_DATASETS"]
