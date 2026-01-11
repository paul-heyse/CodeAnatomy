"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.pyarrow_protocols import ArrayLike, ChunkedArrayLike, TableLike
from arrowdsl.runtime import ExecutionContext
from cpg.builders import PropBuilder
from cpg.kinds import (
    SCIP_ROLE_FORWARD_DEFINITION,
    SCIP_ROLE_GENERATED,
    SCIP_ROLE_TEST,
    EntityKind,
    NodeKind,
)
from cpg.schemas import CPG_PROPS_CONTRACT, CPG_PROPS_SCHEMA, SCHEMA_VERSION, empty_props
from cpg.specs import PropFieldSpec, PropTableSpec, TableGetter


def _expr_context_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if "." in raw:
        raw = raw.rsplit(".", 1)[-1]
    return raw.upper()


def _flag_to_bool(value: object | None) -> bool | None:
    if isinstance(value, bool):
        return True if value else None
    if isinstance(value, int):
        return True if value == 1 else None
    return None


def _set_or_append_column(
    table: TableLike,
    name: str,
    values: ArrayLike | ChunkedArrayLike,
) -> TableLike:
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


def _defs_table(table: TableLike | None) -> TableLike | None:
    if table is None or table.num_rows == 0:
        return None
    if "def_kind" in table.column_names and "kind" in table.column_names:
        def_kind = pc.coalesce(table["def_kind"], table["kind"])
    elif "def_kind" in table.column_names:
        def_kind = table["def_kind"]
    elif "kind" in table.column_names:
        def_kind = table["kind"]
    else:
        def_kind = pa.nulls(table.num_rows, type=pa.string())
    return _set_or_append_column(table, "def_kind_norm", def_kind)


def _scip_role_flags_table(scip_occurrences: TableLike | None) -> TableLike | None:
    if (
        scip_occurrences is None
        or scip_occurrences.num_rows == 0
        or "symbol" not in scip_occurrences.column_names
        or "symbol_roles" not in scip_occurrences.column_names
    ):
        return None

    symbols = pc.cast(scip_occurrences["symbol"], pa.string())
    roles = pc.cast(scip_occurrences["symbol_roles"], pa.int64())
    flag_arrays: list[ArrayLike | ChunkedArrayLike] = []
    flag_names: list[str] = []
    for name, mask, _ in _ROLE_FLAG_SPECS:
        flag = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(mask)), pa.scalar(0))
        flag_arrays.append(pc.cast(flag, pa.int32()))
        flag_names.append(name)

    flags_table = pa.Table.from_arrays([symbols, *flag_arrays], names=["symbol", *flag_names])
    aggs = [(name, "max") for name in flag_names]
    aggregated = flags_table.group_by(["symbol"], use_threads=True).aggregate(aggs)
    return aggregated.rename_columns(["symbol", *flag_names])


def _table_getter(name: str) -> TableGetter:
    def _get_table(tables: Mapping[str, TableLike]) -> TableLike | None:
        return tables.get(name)

    return _get_table


_ROLE_FLAG_SPECS: tuple[tuple[str, int, str], ...] = (
    ("generated", SCIP_ROLE_GENERATED, "scip_role_generated"),
    ("test", SCIP_ROLE_TEST, "scip_role_test"),
    ("forward_definition", SCIP_ROLE_FORWARD_DEFINITION, "scip_role_forward_definition"),
)


PROP_TABLE_SPECS: tuple[PropTableSpec, ...] = (
    PropTableSpec(
        name="file_props",
        option_flag="include_node_props",
        table_getter=_table_getter("repo_files"),
        entity_kind=EntityKind.NODE,
        id_cols=("file_id",),
        node_kind=NodeKind.PY_FILE,
        fields=(
            PropFieldSpec(prop_key="path", source_col="path"),
            PropFieldSpec(prop_key="size_bytes", source_col="size_bytes"),
            PropFieldSpec(prop_key="file_sha256", source_col="file_sha256"),
            PropFieldSpec(prop_key="encoding", source_col="encoding"),
        ),
    ),
    PropTableSpec(
        name="name_ref_props",
        option_flag="include_node_props",
        table_getter=_table_getter("cst_name_refs"),
        entity_kind=EntityKind.NODE,
        id_cols=("name_ref_id",),
        node_kind=NodeKind.CST_NAME_REF,
        fields=(
            PropFieldSpec(prop_key="name", source_col="name"),
            PropFieldSpec(
                prop_key="expr_context",
                source_col="expr_ctx",
                transform=_expr_context_value,
            ),
        ),
    ),
    PropTableSpec(
        name="import_props",
        option_flag="include_node_props",
        table_getter=_table_getter("cst_imports"),
        entity_kind=EntityKind.NODE,
        id_cols=("import_alias_id", "import_id"),
        node_kind=NodeKind.CST_IMPORT_ALIAS,
        fields=(
            PropFieldSpec(prop_key="import_kind", source_col="kind"),
            PropFieldSpec(prop_key="module", source_col="module"),
            PropFieldSpec(prop_key="relative_level", source_col="relative_level"),
            PropFieldSpec(prop_key="imported_name", source_col="name"),
            PropFieldSpec(prop_key="name", source_col="name"),
            PropFieldSpec(prop_key="asname", source_col="asname"),
            PropFieldSpec(prop_key="is_star", source_col="is_star"),
        ),
    ),
    PropTableSpec(
        name="callsite_props",
        option_flag="include_node_props",
        table_getter=_table_getter("cst_callsites"),
        entity_kind=EntityKind.NODE,
        id_cols=("call_id",),
        node_kind=NodeKind.CST_CALLSITE,
        fields=(
            PropFieldSpec(prop_key="callee_shape", source_col="callee_shape"),
            PropFieldSpec(prop_key="callee_text", source_col="callee_text"),
            PropFieldSpec(prop_key="callee_dotted", source_col="callee_dotted"),
            PropFieldSpec(prop_key="arg_count", source_col="arg_count"),
            PropFieldSpec(
                prop_key="callee_qnames",
                source_col="callee_qnames",
                include_if=lambda options: options.include_heavy_json_props,
            ),
        ),
    ),
    PropTableSpec(
        name="def_props",
        option_flag="include_node_props",
        table_getter=_table_getter("cst_defs_norm"),
        entity_kind=EntityKind.NODE,
        id_cols=("def_id",),
        node_kind=NodeKind.CST_DEF,
        fields=(
            PropFieldSpec(prop_key="def_kind", source_col="def_kind_norm"),
            PropFieldSpec(prop_key="name", source_col="name"),
            PropFieldSpec(prop_key="container_def_id", source_col="container_def_id"),
            PropFieldSpec(
                prop_key="qnames",
                source_col="qnames",
                include_if=lambda options: options.include_heavy_json_props,
            ),
        ),
    ),
    PropTableSpec(
        name="qname_props",
        option_flag="include_node_props",
        table_getter=_table_getter("dim_qualified_names"),
        entity_kind=EntityKind.NODE,
        id_cols=("qname_id",),
        node_kind=NodeKind.PY_QUALIFIED_NAME,
        fields=(PropFieldSpec(prop_key="qname", source_col="qname"),),
    ),
    PropTableSpec(
        name="scip_symbol_props",
        option_flag="include_node_props",
        table_getter=_table_getter("scip_symbol_information"),
        entity_kind=EntityKind.NODE,
        id_cols=("symbol",),
        node_kind=NodeKind.SCIP_SYMBOL,
        fields=(
            PropFieldSpec(prop_key="symbol", source_col="symbol"),
            PropFieldSpec(prop_key="display_name", source_col="display_name"),
            PropFieldSpec(prop_key="symbol_kind", source_col="kind"),
            PropFieldSpec(prop_key="enclosing_symbol", source_col="enclosing_symbol"),
            PropFieldSpec(
                prop_key="documentation",
                source_col="documentation",
                include_if=lambda options: options.include_heavy_json_props,
            ),
            PropFieldSpec(
                prop_key="signature_documentation",
                source_col="signature_documentation",
                include_if=lambda options: options.include_heavy_json_props,
            ),
        ),
    ),
    PropTableSpec(
        name="scip_external_symbol_props",
        option_flag="include_node_props",
        table_getter=_table_getter("scip_external_symbol_information"),
        entity_kind=EntityKind.NODE,
        id_cols=("symbol",),
        node_kind=NodeKind.SCIP_SYMBOL,
        fields=(
            PropFieldSpec(prop_key="symbol", source_col="symbol"),
            PropFieldSpec(prop_key="display_name", source_col="display_name"),
            PropFieldSpec(prop_key="symbol_kind", source_col="kind"),
            PropFieldSpec(prop_key="enclosing_symbol", source_col="enclosing_symbol"),
            PropFieldSpec(
                prop_key="documentation",
                source_col="documentation",
                include_if=lambda options: options.include_heavy_json_props,
            ),
            PropFieldSpec(
                prop_key="signature_documentation",
                source_col="signature_documentation",
                include_if=lambda options: options.include_heavy_json_props,
            ),
        ),
    ),
    PropTableSpec(
        name="scip_role_flags",
        option_flag="include_node_props",
        table_getter=_table_getter("scip_role_flags"),
        entity_kind=EntityKind.NODE,
        id_cols=("symbol",),
        fields=tuple(
            PropFieldSpec(
                prop_key=prop_key,
                source_col=flag_name,
                transform=_flag_to_bool,
                skip_if_none=True,
            )
            for flag_name, _, prop_key in _ROLE_FLAG_SPECS
        ),
    ),
    PropTableSpec(
        name="ts_node_props",
        option_flag="include_node_props",
        table_getter=_table_getter("ts_nodes"),
        entity_kind=EntityKind.NODE,
        id_cols=("ts_node_id",),
        node_kind=NodeKind.TS_NODE,
        fields=(
            PropFieldSpec(prop_key="ts_type", source_col="ts_type"),
            PropFieldSpec(prop_key="is_named", source_col="is_named"),
            PropFieldSpec(prop_key="has_error", source_col="has_error"),
        ),
    ),
    PropTableSpec(
        name="ts_error_props",
        option_flag="include_node_props",
        table_getter=_table_getter("ts_errors"),
        entity_kind=EntityKind.NODE,
        id_cols=("ts_error_id",),
        node_kind=NodeKind.TS_ERROR,
        fields=(
            PropFieldSpec(prop_key="ts_type", source_col="ts_type"),
            PropFieldSpec(prop_key="is_error", source_col="is_error"),
        ),
    ),
    PropTableSpec(
        name="ts_missing_props",
        option_flag="include_node_props",
        table_getter=_table_getter("ts_missing"),
        entity_kind=EntityKind.NODE,
        id_cols=("ts_missing_id",),
        node_kind=NodeKind.TS_MISSING,
        fields=(
            PropFieldSpec(prop_key="ts_type", source_col="ts_type"),
            PropFieldSpec(prop_key="is_missing", source_col="is_missing"),
        ),
    ),
    PropTableSpec(
        name="type_expr_props",
        option_flag="include_node_props",
        table_getter=_table_getter("type_exprs_norm"),
        entity_kind=EntityKind.NODE,
        id_cols=("type_expr_id",),
        node_kind=NodeKind.TYPE_EXPR,
        fields=(
            PropFieldSpec(prop_key="expr_text", source_col="expr_text"),
            PropFieldSpec(prop_key="expr_kind", source_col="expr_kind"),
            PropFieldSpec(prop_key="expr_role", source_col="expr_role"),
            PropFieldSpec(prop_key="param_name", source_col="param_name"),
        ),
    ),
    PropTableSpec(
        name="type_props",
        option_flag="include_node_props",
        table_getter=_table_getter("types_norm"),
        entity_kind=EntityKind.NODE,
        id_cols=("type_id",),
        node_kind=NodeKind.TYPE,
        fields=(
            PropFieldSpec(prop_key="type_repr", source_col="type_repr"),
            PropFieldSpec(prop_key="type_form", source_col="type_form"),
            PropFieldSpec(prop_key="origin", source_col="origin"),
        ),
    ),
    PropTableSpec(
        name="diagnostic_props",
        option_flag="include_node_props",
        table_getter=_table_getter("diagnostics_norm"),
        entity_kind=EntityKind.NODE,
        id_cols=("diag_id",),
        node_kind=NodeKind.DIAG,
        fields=(
            PropFieldSpec(prop_key="severity", source_col="severity"),
            PropFieldSpec(prop_key="message", source_col="message"),
            PropFieldSpec(prop_key="diag_source", source_col="diag_source"),
            PropFieldSpec(prop_key="code", source_col="code"),
            PropFieldSpec(prop_key="details", source_col="details"),
        ),
    ),
    PropTableSpec(
        name="runtime_object_props",
        option_flag="include_node_props",
        table_getter=_table_getter("rt_objects"),
        entity_kind=EntityKind.NODE,
        id_cols=("rt_id",),
        node_kind=NodeKind.RT_OBJECT,
        fields=(
            PropFieldSpec(prop_key="module", source_col="module"),
            PropFieldSpec(prop_key="qualname", source_col="qualname"),
            PropFieldSpec(prop_key="name", source_col="name"),
            PropFieldSpec(prop_key="obj_type", source_col="obj_type"),
            PropFieldSpec(prop_key="source_path", source_col="source_path"),
            PropFieldSpec(prop_key="source_line", source_col="source_line"),
        ),
    ),
    PropTableSpec(
        name="runtime_signature_props",
        option_flag="include_node_props",
        table_getter=_table_getter("rt_signatures"),
        entity_kind=EntityKind.NODE,
        id_cols=("sig_id",),
        node_kind=NodeKind.RT_SIGNATURE,
        fields=(
            PropFieldSpec(prop_key="signature", source_col="signature"),
            PropFieldSpec(prop_key="return_annotation", source_col="return_annotation"),
        ),
    ),
    PropTableSpec(
        name="runtime_param_props",
        option_flag="include_node_props",
        table_getter=_table_getter("rt_signature_params"),
        entity_kind=EntityKind.NODE,
        id_cols=("param_id",),
        node_kind=NodeKind.RT_SIGNATURE_PARAM,
        fields=(
            PropFieldSpec(prop_key="name", source_col="name"),
            PropFieldSpec(prop_key="kind", source_col="kind"),
            PropFieldSpec(prop_key="default_repr", source_col="default_repr"),
            PropFieldSpec(prop_key="annotation_repr", source_col="annotation_repr"),
            PropFieldSpec(prop_key="position", source_col="position"),
        ),
    ),
    PropTableSpec(
        name="runtime_member_props",
        option_flag="include_node_props",
        table_getter=_table_getter("rt_members"),
        entity_kind=EntityKind.NODE,
        id_cols=("member_id",),
        node_kind=NodeKind.RT_MEMBER,
        fields=(
            PropFieldSpec(prop_key="name", source_col="name"),
            PropFieldSpec(prop_key="member_kind", source_col="member_kind"),
            PropFieldSpec(prop_key="value_repr", source_col="value_repr"),
            PropFieldSpec(prop_key="value_module", source_col="value_module"),
            PropFieldSpec(prop_key="value_qualname", source_col="value_qualname"),
        ),
    ),
    PropTableSpec(
        name="edge_props",
        option_flag="include_edge_props",
        table_getter=_table_getter("cpg_edges"),
        entity_kind=EntityKind.EDGE,
        id_cols=("edge_id",),
        fields=(
            PropFieldSpec(prop_key="edge_kind", source_col="edge_kind"),
            PropFieldSpec(prop_key="origin", source_col="origin"),
            PropFieldSpec(prop_key="resolution_method", source_col="resolution_method"),
            PropFieldSpec(prop_key="confidence", source_col="confidence"),
            PropFieldSpec(prop_key="score", source_col="score"),
            PropFieldSpec(prop_key="symbol_roles", source_col="symbol_roles"),
            PropFieldSpec(prop_key="qname_source", source_col="qname_source"),
            PropFieldSpec(prop_key="ambiguity_group_id", source_col="ambiguity_group_id"),
            PropFieldSpec(prop_key="rule_name", source_col="rule_name"),
            PropFieldSpec(prop_key="rule_priority", source_col="rule_priority"),
        ),
    ),
)

PROP_BUILDER = PropBuilder(
    table_specs=PROP_TABLE_SPECS,
    schema_version=SCHEMA_VERSION,
    prop_schema=CPG_PROPS_SCHEMA,
)


@dataclass(frozen=True)
class PropsBuildOptions:
    """Configure which property families are emitted."""

    include_node_props: bool = True
    include_edge_props: bool = True
    include_heavy_json_props: bool = True


@dataclass(frozen=True)
class PropsInputTables:
    """Bundle of input tables for property extraction."""

    repo_files: TableLike | None = None
    cst_name_refs: TableLike | None = None
    cst_imports: TableLike | None = None
    cst_callsites: TableLike | None = None
    cst_defs: TableLike | None = None
    dim_qualified_names: TableLike | None = None
    scip_symbol_information: TableLike | None = None
    scip_occurrences: TableLike | None = None
    scip_external_symbol_information: TableLike | None = None
    ts_nodes: TableLike | None = None
    ts_errors: TableLike | None = None
    ts_missing: TableLike | None = None
    type_exprs_norm: TableLike | None = None
    types_norm: TableLike | None = None
    diagnostics_norm: TableLike | None = None
    rt_objects: TableLike | None = None
    rt_signatures: TableLike | None = None
    rt_signature_params: TableLike | None = None
    rt_members: TableLike | None = None
    cpg_edges: TableLike | None = None


def _prop_tables(inputs: PropsInputTables) -> dict[str, TableLike]:
    tables = {
        name: table
        for name, table in {
            "repo_files": inputs.repo_files,
            "cst_name_refs": inputs.cst_name_refs,
            "cst_imports": inputs.cst_imports,
            "cst_callsites": inputs.cst_callsites,
            "dim_qualified_names": inputs.dim_qualified_names,
            "scip_symbol_information": inputs.scip_symbol_information,
            "scip_external_symbol_information": inputs.scip_external_symbol_information,
            "ts_nodes": inputs.ts_nodes,
            "ts_errors": inputs.ts_errors,
            "ts_missing": inputs.ts_missing,
            "type_exprs_norm": inputs.type_exprs_norm,
            "types_norm": inputs.types_norm,
            "diagnostics_norm": inputs.diagnostics_norm,
            "rt_objects": inputs.rt_objects,
            "rt_signatures": inputs.rt_signatures,
            "rt_signature_params": inputs.rt_signature_params,
            "rt_members": inputs.rt_members,
            "cpg_edges": inputs.cpg_edges,
        }.items()
        if table is not None
    }

    if inputs.cst_defs is not None:
        tables["cst_defs"] = inputs.cst_defs
        defs_norm = _defs_table(inputs.cst_defs)
        if defs_norm is not None:
            tables["cst_defs_norm"] = defs_norm

    scip_role_flags = _scip_role_flags_table(inputs.scip_occurrences)
    if scip_role_flags is not None:
        tables["scip_role_flags"] = scip_role_flags
    return tables


def build_cpg_props_raw(
    *,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
) -> TableLike:
    """Build CPG properties without finalization.

    Returns
    -------
    pyarrow.Table
        Raw properties table.
    """
    options = options or PropsBuildOptions()
    tables = _prop_tables(inputs or PropsInputTables())
    out = PROP_BUILDER.build(tables=tables, options=options)

    if out.num_rows == 0:
        return empty_props()

    return out


def build_cpg_props(
    *,
    ctx: ExecutionContext,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
) -> FinalizeResult:
    """Build and finalize CPG properties.

    Returns
    -------
    FinalizeResult
        Finalized properties tables and stats.
    """
    raw = build_cpg_props_raw(
        inputs=inputs,
        options=options,
    )
    return finalize(raw, contract=CPG_PROPS_CONTRACT, ctx=ctx)
