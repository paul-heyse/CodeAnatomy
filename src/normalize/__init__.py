"""Normalization helpers for extracted tables."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.normalize_ids import (
        DEF_USE_EVENT_ID_SPEC,
        DIAG_ID_SPEC,
        REACH_EDGE_ID_SPEC,
        TYPE_EXPR_ID_SPEC,
        TYPE_ID_SPEC,
        hash_spec,
    )
    from datafusion_engine.schema_registry import (
        DIAG_DETAIL_STRUCT,
        DIAG_DETAILS_TYPE,
        DIAG_TAGS_TYPE,
    )
    from normalize.contracts import (
        NORMALIZE_EVIDENCE_NAME,
        normalize_evidence_contract,
        normalize_evidence_schema,
        normalize_evidence_spec,
    )
    from normalize.dataset_fields import field, field_name, fields
    from normalize.dataset_specs import dataset_contract_schema
    from normalize.ibis_api import (
        DiagnosticsSources,
        add_ast_byte_spans,
        add_scip_occurrence_byte_spans,
        anchor_instructions,
        build_cfg_blocks,
        build_cfg_edges,
        build_def_use_events,
        collect_diags,
        normalize_cst_callsites_spans,
        normalize_cst_defs_spans,
        normalize_cst_imports_spans,
        normalize_type_exprs,
        normalize_types,
        run_reaching_defs,
    )
    from normalize.registry_runtime import (
        dataset_alias,
        dataset_contract,
        dataset_input_columns,
        dataset_input_schema,
        dataset_metadata_spec,
        dataset_name_from_alias,
        dataset_names,
        dataset_schema,
        dataset_schema_policy,
        dataset_spec,
        dataset_specs,
        dataset_table_spec,
    )
    from normalize.runner import (
        NormalizeFinalizeSpec,
        NormalizeRunOptions,
        ensure_execution_context,
        run_normalize,
    )
    from normalize.utils import add_span_id_column, span_id

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DEF_USE_EVENT_ID_SPEC": ("datafusion_engine.normalize_ids", "DEF_USE_EVENT_ID_SPEC"),
    "DIAG_DETAILS_TYPE": ("datafusion_engine.schema_registry", "DIAG_DETAILS_TYPE"),
    "DIAG_DETAIL_STRUCT": ("datafusion_engine.schema_registry", "DIAG_DETAIL_STRUCT"),
    "DIAG_ID_SPEC": ("datafusion_engine.normalize_ids", "DIAG_ID_SPEC"),
    "DIAG_TAGS_TYPE": ("datafusion_engine.schema_registry", "DIAG_TAGS_TYPE"),
    "NORMALIZE_EVIDENCE_NAME": ("normalize.contracts", "NORMALIZE_EVIDENCE_NAME"),
    "REACH_EDGE_ID_SPEC": ("datafusion_engine.normalize_ids", "REACH_EDGE_ID_SPEC"),
    "TYPE_EXPR_ID_SPEC": ("datafusion_engine.normalize_ids", "TYPE_EXPR_ID_SPEC"),
    "TYPE_ID_SPEC": ("datafusion_engine.normalize_ids", "TYPE_ID_SPEC"),
    "DiagnosticsSources": ("normalize.ibis_api", "DiagnosticsSources"),
    "NormalizeFinalizeSpec": ("normalize.runner", "NormalizeFinalizeSpec"),
    "NormalizeRunOptions": ("normalize.runner", "NormalizeRunOptions"),
    "add_ast_byte_spans": ("normalize.ibis_api", "add_ast_byte_spans"),
    "add_scip_occurrence_byte_spans": ("normalize.ibis_api", "add_scip_occurrence_byte_spans"),
    "add_span_id_column": ("normalize.utils", "add_span_id_column"),
    "anchor_instructions": ("normalize.ibis_api", "anchor_instructions"),
    "build_cfg_blocks": ("normalize.ibis_api", "build_cfg_blocks"),
    "build_cfg_edges": ("normalize.ibis_api", "build_cfg_edges"),
    "build_def_use_events": ("normalize.ibis_api", "build_def_use_events"),
    "collect_diags": ("normalize.ibis_api", "collect_diags"),
    "dataset_alias": ("normalize.registry_runtime", "dataset_alias"),
    "dataset_contract": ("normalize.registry_runtime", "dataset_contract"),
    "dataset_contract_schema": ("normalize.dataset_specs", "dataset_contract_schema"),
    "dataset_input_columns": ("normalize.registry_runtime", "dataset_input_columns"),
    "dataset_input_schema": ("normalize.registry_runtime", "dataset_input_schema"),
    "dataset_metadata_spec": ("normalize.registry_runtime", "dataset_metadata_spec"),
    "dataset_name_from_alias": ("normalize.registry_runtime", "dataset_name_from_alias"),
    "dataset_names": ("normalize.registry_runtime", "dataset_names"),
    "dataset_schema": ("normalize.registry_runtime", "dataset_schema"),
    "dataset_schema_policy": ("normalize.registry_runtime", "dataset_schema_policy"),
    "dataset_spec": ("normalize.registry_runtime", "dataset_spec"),
    "dataset_specs": ("normalize.registry_runtime", "dataset_specs"),
    "dataset_table_spec": ("normalize.registry_runtime", "dataset_table_spec"),
    "ensure_execution_context": ("normalize.runner", "ensure_execution_context"),
    "field": ("normalize.dataset_fields", "field"),
    "field_name": ("normalize.dataset_fields", "field_name"),
    "fields": ("normalize.dataset_fields", "fields"),
    "hash_spec": ("datafusion_engine.normalize_ids", "hash_spec"),
    "normalize_cst_callsites_spans": (
        "normalize.ibis_api",
        "normalize_cst_callsites_spans",
    ),
    "normalize_cst_defs_spans": ("normalize.ibis_api", "normalize_cst_defs_spans"),
    "normalize_cst_imports_spans": (
        "normalize.ibis_api",
        "normalize_cst_imports_spans",
    ),
    "normalize_evidence_contract": ("normalize.contracts", "normalize_evidence_contract"),
    "normalize_evidence_schema": ("normalize.contracts", "normalize_evidence_schema"),
    "normalize_evidence_spec": ("normalize.contracts", "normalize_evidence_spec"),
    "normalize_type_exprs": ("normalize.ibis_api", "normalize_type_exprs"),
    "normalize_types": ("normalize.ibis_api", "normalize_types"),
    "run_normalize": ("normalize.runner", "run_normalize"),
    "run_reaching_defs": ("normalize.ibis_api", "run_reaching_defs"),
    "span_id": ("normalize.utils", "span_id"),
}


def __getattr__(name: str) -> object:
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_EXPORT_MAP))


__all__ = [
    "DEF_USE_EVENT_ID_SPEC",
    "DIAG_DETAILS_TYPE",
    "DIAG_DETAIL_STRUCT",
    "DIAG_ID_SPEC",
    "DIAG_TAGS_TYPE",
    "NORMALIZE_EVIDENCE_NAME",
    "REACH_EDGE_ID_SPEC",
    "TYPE_EXPR_ID_SPEC",
    "TYPE_ID_SPEC",
    "DiagnosticsSources",
    "NormalizeFinalizeSpec",
    "NormalizeRunOptions",
    "add_ast_byte_spans",
    "add_scip_occurrence_byte_spans",
    "add_span_id_column",
    "anchor_instructions",
    "build_cfg_blocks",
    "build_cfg_edges",
    "build_def_use_events",
    "collect_diags",
    "dataset_alias",
    "dataset_contract",
    "dataset_contract_schema",
    "dataset_input_columns",
    "dataset_input_schema",
    "dataset_metadata_spec",
    "dataset_name_from_alias",
    "dataset_names",
    "dataset_schema",
    "dataset_schema_policy",
    "dataset_spec",
    "dataset_specs",
    "dataset_table_spec",
    "ensure_execution_context",
    "field",
    "field_name",
    "fields",
    "hash_spec",
    "normalize_cst_callsites_spans",
    "normalize_cst_defs_spans",
    "normalize_cst_imports_spans",
    "normalize_evidence_contract",
    "normalize_evidence_schema",
    "normalize_evidence_spec",
    "normalize_type_exprs",
    "normalize_types",
    "run_normalize",
    "run_reaching_defs",
    "span_id",
]
