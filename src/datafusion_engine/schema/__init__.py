"""Canonical schema exports with lazy loading to avoid import cycles."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__all__ = [
    "AST_CORE_VIEW_NAMES",
    "AST_FILES_SCHEMA",
    "AST_OPTIONAL_VIEW_NAMES",
    "AST_VIEW_NAMES",
    "BYTECODE_FILES_SCHEMA",
    "CST_VIEW_NAMES",
    "DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA",
    "DATAFUSION_PLAN_ARTIFACTS_SCHEMA",
    "LIBCST_FILES_SCHEMA",
    "PIPELINE_PLAN_DRIFT_SCHEMA",
    "PIPELINE_TASK_EXPANSION_SCHEMA",
    "PIPELINE_TASK_GROUPING_SCHEMA",
    "PIPELINE_TASK_SUBMISSION_SCHEMA",
    "REPO_FILES_SCHEMA",
    "SCHEMA_META_NAME",
    "SCHEMA_META_VERSION",
    "SCIP_DOCUMENT_SYMBOLS_SCHEMA",
    "SCIP_INDEX_SCHEMA",
    "SCIP_METADATA_SCHEMA",
    "SCIP_OCCURRENCES_SCHEMA",
    "SYMTABLE_FILES_SCHEMA",
    "TREE_SITTER_CHECK_VIEWS",
    "TREE_SITTER_FILES_SCHEMA",
    "TREE_SITTER_VIEW_NAMES",
    "base_extract_schema_registry",
    "default_attrs_value",
    "extract_base_schema_for",
    "extract_base_schema_names",
    "extract_nested_dataset_names",
    "extract_nested_schema_names",
    "extract_schema_contract_for",
    "extract_schema_for",
    "extraction_schemas",
    "missing_schema_names",
    "nested_base_df",
    "nested_dataset_names",
    "nested_dataset_registry",
    "nested_path_for",
    "nested_schema_names",
    "nested_view_spec",
    "nested_view_specs",
    "nested_views",
    "observability_schemas",
    "registered_table_names",
    "relationship_schema_for",
    "relationship_schema_names",
    "relationship_schema_registry",
    "root_identity_registry",
    "schema_contract_for_table",
    "validate_ast_views",
    "validate_nested_types",
    "validate_required_bytecode_functions",
    "validate_required_cst_functions",
    "validate_required_engine_functions",
    "validate_required_symtable_functions",
    "validate_semantic_types",
    "validate_udf_info_schema_parity",
]

_MODULE_ALIASES: dict[str, str] = {
    "extraction_schemas": "datafusion_engine.schema.extraction_schemas",
    "nested_views": "datafusion_engine.schema.nested_views",
    "observability_schemas": "datafusion_engine.schema.observability_schemas",
}

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "SCHEMA_META_NAME": ("arrow_utils.core.schema_constants", "SCHEMA_META_NAME"),
    "SCHEMA_META_VERSION": ("arrow_utils.core.schema_constants", "SCHEMA_META_VERSION"),
    "AST_CORE_VIEW_NAMES": ("datafusion_engine.schema.extraction_schemas", "AST_CORE_VIEW_NAMES"),
    "AST_FILES_SCHEMA": ("datafusion_engine.schema.extraction_schemas", "AST_FILES_SCHEMA"),
    "AST_OPTIONAL_VIEW_NAMES": (
        "datafusion_engine.schema.extraction_schemas",
        "AST_OPTIONAL_VIEW_NAMES",
    ),
    "AST_VIEW_NAMES": ("datafusion_engine.schema.extraction_schemas", "AST_VIEW_NAMES"),
    "BYTECODE_FILES_SCHEMA": (
        "datafusion_engine.schema.extraction_schemas",
        "BYTECODE_FILES_SCHEMA",
    ),
    "CST_VIEW_NAMES": ("datafusion_engine.schema.extraction_schemas", "CST_VIEW_NAMES"),
    "LIBCST_FILES_SCHEMA": ("datafusion_engine.schema.extraction_schemas", "LIBCST_FILES_SCHEMA"),
    "REPO_FILES_SCHEMA": ("datafusion_engine.schema.extraction_schemas", "REPO_FILES_SCHEMA"),
    "SCIP_DOCUMENT_SYMBOLS_SCHEMA": (
        "datafusion_engine.schema.extraction_schemas",
        "SCIP_DOCUMENT_SYMBOLS_SCHEMA",
    ),
    "SCIP_INDEX_SCHEMA": ("datafusion_engine.schema.extraction_schemas", "SCIP_INDEX_SCHEMA"),
    "SCIP_METADATA_SCHEMA": ("datafusion_engine.schema.extraction_schemas", "SCIP_METADATA_SCHEMA"),
    "SCIP_OCCURRENCES_SCHEMA": (
        "datafusion_engine.schema.extraction_schemas",
        "SCIP_OCCURRENCES_SCHEMA",
    ),
    "SYMTABLE_FILES_SCHEMA": (
        "datafusion_engine.schema.extraction_schemas",
        "SYMTABLE_FILES_SCHEMA",
    ),
    "TREE_SITTER_CHECK_VIEWS": (
        "datafusion_engine.schema.extraction_schemas",
        "TREE_SITTER_CHECK_VIEWS",
    ),
    "TREE_SITTER_FILES_SCHEMA": (
        "datafusion_engine.schema.extraction_schemas",
        "TREE_SITTER_FILES_SCHEMA",
    ),
    "TREE_SITTER_VIEW_NAMES": (
        "datafusion_engine.schema.extraction_schemas",
        "TREE_SITTER_VIEW_NAMES",
    ),
    "base_extract_schema_registry": (
        "datafusion_engine.schema.extraction_schemas",
        "base_extract_schema_registry",
    ),
    "default_attrs_value": ("datafusion_engine.schema.extraction_schemas", "default_attrs_value"),
    "extract_base_schema_for": (
        "datafusion_engine.schema.extraction_schemas",
        "extract_base_schema_for",
    ),
    "extract_base_schema_names": (
        "datafusion_engine.schema.extraction_schemas",
        "extract_base_schema_names",
    ),
    "extract_schema_contract_for": (
        "datafusion_engine.schema.extraction_schemas",
        "extract_schema_contract_for",
    ),
    "extract_schema_for": ("datafusion_engine.schema.extraction_schemas", "extract_schema_for"),
    "relationship_schema_for": (
        "datafusion_engine.schema.extraction_schemas",
        "relationship_schema_for",
    ),
    "relationship_schema_names": (
        "datafusion_engine.schema.extraction_schemas",
        "relationship_schema_names",
    ),
    "relationship_schema_registry": (
        "datafusion_engine.schema.extraction_schemas",
        "relationship_schema_registry",
    ),
    "extract_nested_dataset_names": (
        "datafusion_engine.schema.nested_views",
        "extract_nested_dataset_names",
    ),
    "extract_nested_schema_names": (
        "datafusion_engine.schema.nested_views",
        "extract_nested_schema_names",
    ),
    "nested_base_df": ("datafusion_engine.schema.nested_views", "nested_base_df"),
    "nested_dataset_names": ("datafusion_engine.schema.nested_views", "nested_dataset_names"),
    "nested_dataset_registry": (
        "datafusion_engine.schema.nested_views",
        "nested_dataset_registry",
    ),
    "nested_path_for": ("datafusion_engine.schema.nested_views", "nested_path_for"),
    "nested_schema_names": ("datafusion_engine.schema.nested_views", "nested_schema_names"),
    "nested_view_spec": ("datafusion_engine.schema.nested_views", "nested_view_spec"),
    "nested_view_specs": ("datafusion_engine.schema.nested_views", "nested_view_specs"),
    "root_identity_registry": ("datafusion_engine.schema.nested_views", "root_identity_registry"),
    "validate_nested_types": ("datafusion_engine.schema.nested_views", "validate_nested_types"),
    "DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA": (
        "datafusion_engine.schema.observability_schemas",
        "DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA",
    ),
    "DATAFUSION_PLAN_ARTIFACTS_SCHEMA": (
        "datafusion_engine.schema.observability_schemas",
        "DATAFUSION_PLAN_ARTIFACTS_SCHEMA",
    ),
    "PIPELINE_PLAN_DRIFT_SCHEMA": (
        "datafusion_engine.schema.observability_schemas",
        "PIPELINE_PLAN_DRIFT_SCHEMA",
    ),
    "PIPELINE_TASK_EXPANSION_SCHEMA": (
        "datafusion_engine.schema.observability_schemas",
        "PIPELINE_TASK_EXPANSION_SCHEMA",
    ),
    "PIPELINE_TASK_GROUPING_SCHEMA": (
        "datafusion_engine.schema.observability_schemas",
        "PIPELINE_TASK_GROUPING_SCHEMA",
    ),
    "PIPELINE_TASK_SUBMISSION_SCHEMA": (
        "datafusion_engine.schema.observability_schemas",
        "PIPELINE_TASK_SUBMISSION_SCHEMA",
    ),
    "missing_schema_names": (
        "datafusion_engine.schema.observability_schemas",
        "missing_schema_names",
    ),
    "registered_table_names": (
        "datafusion_engine.schema.observability_schemas",
        "registered_table_names",
    ),
    "schema_contract_for_table": (
        "datafusion_engine.schema.observability_schemas",
        "schema_contract_for_table",
    ),
    "validate_ast_views": ("datafusion_engine.schema.observability_schemas", "validate_ast_views"),
    "validate_required_bytecode_functions": (
        "datafusion_engine.schema.observability_schemas",
        "validate_required_bytecode_functions",
    ),
    "validate_required_cst_functions": (
        "datafusion_engine.schema.observability_schemas",
        "validate_required_cst_functions",
    ),
    "validate_required_engine_functions": (
        "datafusion_engine.schema.observability_schemas",
        "validate_required_engine_functions",
    ),
    "validate_required_symtable_functions": (
        "datafusion_engine.schema.observability_schemas",
        "validate_required_symtable_functions",
    ),
    "validate_semantic_types": (
        "datafusion_engine.schema.observability_schemas",
        "validate_semantic_types",
    ),
    "validate_udf_info_schema_parity": (
        "datafusion_engine.schema.observability_schemas",
        "validate_udf_info_schema_parity",
    ),
}

if TYPE_CHECKING:
    from arrow_utils.core.schema_constants import SCHEMA_META_NAME, SCHEMA_META_VERSION
    from datafusion_engine.schema import extraction_schemas, nested_views, observability_schemas
    from datafusion_engine.schema.extraction_schemas import (
        AST_CORE_VIEW_NAMES,
        AST_FILES_SCHEMA,
        AST_OPTIONAL_VIEW_NAMES,
        AST_VIEW_NAMES,
        BYTECODE_FILES_SCHEMA,
        CST_VIEW_NAMES,
        LIBCST_FILES_SCHEMA,
        REPO_FILES_SCHEMA,
        SCIP_DOCUMENT_SYMBOLS_SCHEMA,
        SCIP_INDEX_SCHEMA,
        SCIP_METADATA_SCHEMA,
        SCIP_OCCURRENCES_SCHEMA,
        SYMTABLE_FILES_SCHEMA,
        TREE_SITTER_CHECK_VIEWS,
        TREE_SITTER_FILES_SCHEMA,
        TREE_SITTER_VIEW_NAMES,
        base_extract_schema_registry,
        default_attrs_value,
        extract_base_schema_for,
        extract_base_schema_names,
        extract_schema_contract_for,
        extract_schema_for,
        relationship_schema_for,
        relationship_schema_names,
        relationship_schema_registry,
    )
    from datafusion_engine.schema.nested_views import (
        extract_nested_dataset_names,
        extract_nested_schema_names,
        nested_base_df,
        nested_dataset_names,
        nested_dataset_registry,
        nested_path_for,
        nested_schema_names,
        nested_view_spec,
        nested_view_specs,
        root_identity_registry,
        validate_nested_types,
    )
    from datafusion_engine.schema.observability_schemas import (
        DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA,
        DATAFUSION_PLAN_ARTIFACTS_SCHEMA,
        PIPELINE_PLAN_DRIFT_SCHEMA,
        PIPELINE_TASK_EXPANSION_SCHEMA,
        PIPELINE_TASK_GROUPING_SCHEMA,
        PIPELINE_TASK_SUBMISSION_SCHEMA,
        missing_schema_names,
        registered_table_names,
        schema_contract_for_table,
        validate_ast_views,
        validate_required_bytecode_functions,
        validate_required_cst_functions,
        validate_required_engine_functions,
        validate_required_symtable_functions,
        validate_semantic_types,
        validate_udf_info_schema_parity,
    )


def __getattr__(name: str) -> object:
    module_name = _MODULE_ALIASES.get(name)
    if module_name is not None:
        module = importlib.import_module(module_name)
        globals()[name] = module
        return module
    export = _EXPORT_MAP.get(name)
    if export is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr = export
    module = importlib.import_module(module_name)
    value = getattr(module, attr)
    globals()[name] = value
    return value
