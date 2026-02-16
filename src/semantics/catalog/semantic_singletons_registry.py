"""Registry for singleton semantic datasets not covered by other registries."""

from __future__ import annotations

from typing import Final

from semantics.catalog.dataset_rows import SEMANTIC_SCHEMA_VERSION, SemanticDatasetRow
from semantics.naming import canonical_output_name
from semantics.output_names import RELATION_OUTPUT_NAME, RELATION_OUTPUT_ORDERING_KEYS

_RELATION_OUTPUT_FIELDS: tuple[str, ...] = (
    "src",
    "dst",
    "path",
    "edge_owner_file_id",
    "bstart",
    "bend",
    "origin",
    "resolution_method",
    "binding_kind",
    "def_site_kind",
    "use_kind",
    "kind",
    "reason",
    "confidence",
    "score",
    "symbol_roles",
    "qname_source",
    "ambiguity_group_id",
    "diag_source",
    "severity",
    "task_name",
    "task_priority",
)

_RELATION_OUTPUT_KEYS: tuple[str, ...] = tuple(key for key, _order in RELATION_OUTPUT_ORDERING_KEYS)

SEMANTIC_SINGLETON_DATASETS: Final[tuple[SemanticDatasetRow, ...]] = (
    SemanticDatasetRow(
        name=canonical_output_name("scip_occurrences_norm"),
        version=SEMANTIC_SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=(
            "symbol",
            "symbol_roles",
            "bstart",
            "bend",
            "is_definition",
            "is_read",
            "is_import",
        ),
        category="semantic",
        supports_cdf=True,
        partition_cols=(),
        merge_keys=("path", "bstart", "bend", "symbol"),
        join_keys=("path", "bstart", "bend"),
        template="semantic_scip",
        view_builder="scip_occurrences_norm_df_builder",
        register_view=True,
        source_dataset="scip_occurrences",
    ),
    SemanticDatasetRow(
        name="file_quality",
        version=SEMANTIC_SCHEMA_VERSION,
        bundles=(),
        fields=(
            "file_id",
            "file_sha256",
            "has_cst_parse_errors",
            "cst_error_count",
            "ts_timed_out",
            "ts_error_count",
            "ts_missing_count",
            "ts_match_limit_exceeded",
            "has_scip_diagnostics",
            "scip_diagnostic_count",
            "scip_encoding_unspecified",
            "file_quality_score",
        ),
        category="analysis",
        supports_cdf=True,
        partition_cols=(),
        merge_keys=("file_id",),
        join_keys=("file_id",),
        template="file_quality",
        view_builder="file_quality_df_builder",
        register_view=True,
        source_dataset=None,
    ),
    SemanticDatasetRow(
        name=RELATION_OUTPUT_NAME,
        version=SEMANTIC_SCHEMA_VERSION,
        bundles=(),
        fields=_RELATION_OUTPUT_FIELDS,
        category="semantic",
        supports_cdf=True,
        partition_cols=(),
        merge_keys=_RELATION_OUTPUT_KEYS,
        join_keys=_RELATION_OUTPUT_KEYS,
        template="relation_output",
        view_builder=None,
        register_view=False,
        source_dataset=None,
    ),
)

__all__ = ["SEMANTIC_SINGLETON_DATASETS"]
