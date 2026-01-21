"""Normalization helpers for extracted tables."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from normalize.contracts import (
        NORMALIZE_EVIDENCE_NAME,
        normalize_evidence_contract,
        normalize_evidence_schema,
        normalize_evidence_spec,
    )
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
    from normalize.registry_fields import (
        DIAG_DETAIL_STRUCT,
        DIAG_DETAILS_TYPE,
        DIAG_TAGS_TYPE,
        field,
        field_name,
        fields,
    )
    from normalize.registry_ids import (
        DEF_USE_EVENT_ID_SPEC,
        DIAG_ID_SPEC,
        REACH_EDGE_ID_SPEC,
        TYPE_EXPR_ID_SPEC,
        TYPE_ID_SPEC,
        hash_spec,
    )
    from normalize.registry_specs import (
        dataset_contract,
        dataset_input_columns,
        dataset_input_schema,
        dataset_metadata_spec,
        dataset_names,
        dataset_schema,
        dataset_schema_policy,
        dataset_spec,
        dataset_specs,
        dataset_table_spec,
    )
    from normalize.runner import (
        NormalizeFinalizeSpec,
        NormalizeIbisPlanOptions,
        NormalizeRuleCompilation,
        NormalizeRunOptions,
        ResolvedNormalizeRule,
        compile_normalize_plans_ibis,
        ensure_canonical,
        ensure_execution_context,
        resolve_normalize_rules,
        run_normalize,
    )
    from normalize.utils import add_span_id_column, span_id
    from relspec.model import AmbiguityPolicy, ConfidencePolicy
    from relspec.rules.definitions import EvidenceSpec, ExecutionMode

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DEF_USE_EVENT_ID_SPEC": ("normalize.registry_ids", "DEF_USE_EVENT_ID_SPEC"),
    "DIAG_DETAILS_TYPE": ("normalize.registry_fields", "DIAG_DETAILS_TYPE"),
    "DIAG_DETAIL_STRUCT": ("normalize.registry_fields", "DIAG_DETAIL_STRUCT"),
    "DIAG_ID_SPEC": ("normalize.registry_ids", "DIAG_ID_SPEC"),
    "DIAG_TAGS_TYPE": ("normalize.registry_fields", "DIAG_TAGS_TYPE"),
    "NORMALIZE_EVIDENCE_NAME": ("normalize.contracts", "NORMALIZE_EVIDENCE_NAME"),
    "REACH_EDGE_ID_SPEC": ("normalize.registry_ids", "REACH_EDGE_ID_SPEC"),
    "TYPE_EXPR_ID_SPEC": ("normalize.registry_ids", "TYPE_EXPR_ID_SPEC"),
    "TYPE_ID_SPEC": ("normalize.registry_ids", "TYPE_ID_SPEC"),
    "AmbiguityPolicy": ("relspec.model", "AmbiguityPolicy"),
    "ConfidencePolicy": ("relspec.model", "ConfidencePolicy"),
    "DiagnosticsSources": ("normalize.ibis_api", "DiagnosticsSources"),
    "EvidenceSpec": ("relspec.rules.definitions", "EvidenceSpec"),
    "ExecutionMode": ("relspec.rules.definitions", "ExecutionMode"),
    "NormalizeFinalizeSpec": ("normalize.runner", "NormalizeFinalizeSpec"),
    "NormalizeIbisPlanOptions": ("normalize.runner", "NormalizeIbisPlanOptions"),
    "ResolvedNormalizeRule": ("normalize.runner", "ResolvedNormalizeRule"),
    "NormalizeRuleCompilation": ("normalize.runner", "NormalizeRuleCompilation"),
    "NormalizeRunOptions": ("normalize.runner", "NormalizeRunOptions"),
    "add_ast_byte_spans": ("normalize.ibis_api", "add_ast_byte_spans"),
    "add_scip_occurrence_byte_spans": ("normalize.ibis_api", "add_scip_occurrence_byte_spans"),
    "add_span_id_column": ("normalize.utils", "add_span_id_column"),
    "anchor_instructions": ("normalize.ibis_api", "anchor_instructions"),
    "build_cfg_blocks": ("normalize.ibis_api", "build_cfg_blocks"),
    "build_cfg_edges": ("normalize.ibis_api", "build_cfg_edges"),
    "build_def_use_events": ("normalize.ibis_api", "build_def_use_events"),
    "collect_diags": ("normalize.ibis_api", "collect_diags"),
    "compile_normalize_plans_ibis": ("normalize.runner", "compile_normalize_plans_ibis"),
    "dataset_contract": ("normalize.registry_specs", "dataset_contract"),
    "dataset_input_columns": ("normalize.registry_specs", "dataset_input_columns"),
    "dataset_input_schema": ("normalize.registry_specs", "dataset_input_schema"),
    "dataset_metadata_spec": ("normalize.registry_specs", "dataset_metadata_spec"),
    "dataset_names": ("normalize.registry_specs", "dataset_names"),
    "dataset_schema": ("normalize.registry_specs", "dataset_schema"),
    "dataset_schema_policy": ("normalize.registry_specs", "dataset_schema_policy"),
    "dataset_spec": ("normalize.registry_specs", "dataset_spec"),
    "dataset_specs": ("normalize.registry_specs", "dataset_specs"),
    "dataset_table_spec": ("normalize.registry_specs", "dataset_table_spec"),
    "ensure_canonical": ("normalize.runner", "ensure_canonical"),
    "ensure_execution_context": ("normalize.runner", "ensure_execution_context"),
    "resolve_normalize_rules": ("normalize.runner", "resolve_normalize_rules"),
    "field": ("normalize.registry_fields", "field"),
    "field_name": ("normalize.registry_fields", "field_name"),
    "fields": ("normalize.registry_fields", "fields"),
    "hash_spec": ("normalize.registry_ids", "hash_spec"),
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
    "AmbiguityPolicy",
    "ConfidencePolicy",
    "DiagnosticsSources",
    "EvidenceSpec",
    "ExecutionMode",
    "NormalizeFinalizeSpec",
    "NormalizeIbisPlanOptions",
    "NormalizeRuleCompilation",
    "NormalizeRunOptions",
    "ResolvedNormalizeRule",
    "add_ast_byte_spans",
    "add_scip_occurrence_byte_spans",
    "add_span_id_column",
    "anchor_instructions",
    "build_cfg_blocks",
    "build_cfg_edges",
    "build_def_use_events",
    "collect_diags",
    "compile_normalize_plans_ibis",
    "dataset_contract",
    "dataset_input_columns",
    "dataset_input_schema",
    "dataset_metadata_spec",
    "dataset_names",
    "dataset_schema",
    "dataset_schema_policy",
    "dataset_spec",
    "dataset_specs",
    "dataset_table_spec",
    "ensure_canonical",
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
    "resolve_normalize_rules",
    "run_normalize",
    "run_reaching_defs",
    "span_id",
]
