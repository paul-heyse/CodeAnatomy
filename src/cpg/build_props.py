"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.empty import empty_table
from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.runtime import ExecutionContext
from cpg.kinds import EntityKind, NodeKind
from cpg.schemas import CPG_PROPS_CONTRACT, CPG_PROPS_SCHEMA, SCHEMA_VERSION

type PropValue = object | None
type PropRow = dict[str, object]


def _row_id(row: Mapping[str, object], key: str) -> str | None:
    value = row.get(key)
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int) and not isinstance(value, bool) and value:
        return str(value)
    return None


def _expr_context_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if "." in raw:
        raw = raw.rsplit(".", 1)[-1]
    return raw.upper()


def _add_prop(
    rows: list[PropRow],
    *,
    entity_kind: EntityKind,
    entity_id: str,
    key: str,
    value: PropValue,
) -> None:
    rec: PropRow = {
        "schema_version": SCHEMA_VERSION,
        "entity_kind": entity_kind.value,
        "entity_id": str(entity_id),
        "prop_key": str(key),
        "value_str": None,
        "value_int": None,
        "value_float": None,
        "value_bool": None,
        "value_json": None,
    }

    if value is None:
        rows.append(rec)
        return

    if isinstance(value, bool):
        rec["value_bool"] = bool(value)
    elif isinstance(value, int) and not isinstance(value, bool):
        rec["value_int"] = int(value)
    elif isinstance(value, float):
        rec["value_float"] = float(value)
    elif isinstance(value, str):
        rec["value_str"] = value
    else:
        try:
            rec["value_json"] = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            rec["value_json"] = json.dumps(str(value), ensure_ascii=False)
    rows.append(rec)


@dataclass(frozen=True)
class PropsBuildOptions:
    """Configure which property families are emitted."""

    include_node_props: bool = True
    include_edge_props: bool = True
    include_heavy_json_props: bool = True


@dataclass(frozen=True)
class PropsInputTables:
    """Bundle of input tables for property extraction."""

    repo_files: pa.Table | None = None
    cst_name_refs: pa.Table | None = None
    cst_imports: pa.Table | None = None
    cst_callsites: pa.Table | None = None
    cst_defs: pa.Table | None = None
    dim_qualified_names: pa.Table | None = None
    scip_symbol_information: pa.Table | None = None
    ts_nodes: pa.Table | None = None
    ts_errors: pa.Table | None = None
    ts_missing: pa.Table | None = None
    type_exprs_norm: pa.Table | None = None
    types_norm: pa.Table | None = None
    diagnostics_norm: pa.Table | None = None
    rt_objects: pa.Table | None = None
    rt_signatures: pa.Table | None = None
    rt_signature_params: pa.Table | None = None
    rt_members: pa.Table | None = None
    cpg_edges: pa.Table | None = None


def _add_file_props(rows: list[PropRow], repo_files: pa.Table | None) -> None:
    if repo_files is None or repo_files.num_rows == 0:
        return
    for row in repo_files.to_pylist():
        file_id = _row_id(row, "file_id")
        if file_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id,
            key="node_kind",
            value=NodeKind.PY_FILE.value,
        )
        _add_prop(
            rows, entity_kind=EntityKind.NODE, entity_id=file_id, key="path", value=row.get("path")
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id,
            key="size_bytes",
            value=row.get("size_bytes"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id,
            key="file_sha256",
            value=row.get("file_sha256"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id,
            key="encoding",
            value=row.get("encoding"),
        )


def _add_name_ref_props(rows: list[PropRow], cst_name_refs: pa.Table | None) -> None:
    if cst_name_refs is None or cst_name_refs.num_rows == 0:
        return
    for row in cst_name_refs.to_pylist():
        name_id = _row_id(row, "name_ref_id")
        if name_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=name_id,
            key="node_kind",
            value=NodeKind.CST_NAME_REF.value,
        )
        _add_prop(
            rows, entity_kind=EntityKind.NODE, entity_id=name_id, key="name", value=row.get("name")
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=name_id,
            key="expr_context",
            value=_expr_context_value(row.get("expr_ctx")),
        )


def _add_import_props(rows: list[PropRow], cst_imports: pa.Table | None) -> None:
    if cst_imports is None or cst_imports.num_rows == 0:
        return
    id_key = "import_alias_id" if "import_alias_id" in cst_imports.column_names else "import_id"
    for row in cst_imports.to_pylist():
        import_id = _row_id(row, id_key)
        if import_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id,
            key="node_kind",
            value=NodeKind.CST_IMPORT_ALIAS.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id,
            key="import_kind",
            value=row.get("kind"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id,
            key="module",
            value=row.get("module"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id,
            key="relative_level",
            value=row.get("relative_level"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id,
            key="imported_name",
            value=row.get("name"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id,
            key="name",
            value=row.get("name"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id,
            key="asname",
            value=row.get("asname"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id,
            key="is_star",
            value=row.get("is_star"),
        )


def _add_callsite_props(
    rows: list[PropRow],
    cst_callsites: pa.Table | None,
    *,
    include_heavy_json: bool,
) -> None:
    if cst_callsites is None or cst_callsites.num_rows == 0:
        return
    for row in cst_callsites.to_pylist():
        call_id = _row_id(row, "call_id")
        if call_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id,
            key="node_kind",
            value=NodeKind.CST_CALLSITE.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id,
            key="callee_shape",
            value=row.get("callee_shape"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id,
            key="callee_text",
            value=row.get("callee_text"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id,
            key="callee_dotted",
            value=row.get("callee_dotted"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id,
            key="arg_count",
            value=row.get("arg_count"),
        )
        if include_heavy_json:
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=call_id,
                key="callee_qnames",
                value=row.get("callee_qnames"),
            )


def _add_def_props(
    rows: list[PropRow],
    cst_defs: pa.Table | None,
    *,
    include_heavy_json: bool,
) -> None:
    if cst_defs is None or cst_defs.num_rows == 0:
        return
    for row in cst_defs.to_pylist():
        def_id = _row_id(row, "def_id")
        if def_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=def_id,
            key="node_kind",
            value=NodeKind.CST_DEF.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=def_id,
            key="def_kind",
            value=row.get("def_kind") or row.get("kind"),
        )
        _add_prop(
            rows, entity_kind=EntityKind.NODE, entity_id=def_id, key="name", value=row.get("name")
        )
        if include_heavy_json:
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=def_id,
                key="qnames",
                value=row.get("qnames"),
            )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=def_id,
            key="container_def_id",
            value=row.get("container_def_id"),
        )


def _add_qname_props(rows: list[PropRow], dim_qualified_names: pa.Table | None) -> None:
    if (
        dim_qualified_names is None
        or dim_qualified_names.num_rows == 0
        or "qname_id" not in dim_qualified_names.column_names
    ):
        return
    for row in dim_qualified_names.to_pylist():
        qname_id = _row_id(row, "qname_id")
        if qname_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=qname_id,
            key="node_kind",
            value=NodeKind.PY_QUALIFIED_NAME.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=qname_id,
            key="qname",
            value=row.get("qname"),
        )


def _add_symbol_props(
    rows: list[PropRow],
    scip_symbol_information: pa.Table | None,
    *,
    include_heavy_json: bool,
) -> None:
    if scip_symbol_information is None or scip_symbol_information.num_rows == 0:
        return
    for row in scip_symbol_information.to_pylist():
        symbol = _row_id(row, "symbol")
        if symbol is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol,
            key="node_kind",
            value=NodeKind.SCIP_SYMBOL.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol,
            key="symbol",
            value=row.get("symbol"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol,
            key="display_name",
            value=row.get("display_name"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol,
            key="symbol_kind",
            value=row.get("kind"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol,
            key="enclosing_symbol",
            value=row.get("enclosing_symbol"),
        )
        if include_heavy_json:
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=symbol,
                key="documentation",
                value=row.get("documentation"),
            )


def _add_ts_props(
    rows: list[PropRow],
    ts_nodes: pa.Table | None,
    ts_errors: pa.Table | None,
    ts_missing: pa.Table | None,
) -> None:
    if ts_nodes is not None and ts_nodes.num_rows:
        for row in ts_nodes.to_pylist():
            node_id = _row_id(row, "ts_node_id")
            if node_id is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="node_kind",
                value=NodeKind.TS_NODE.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="ts_type",
                value=row.get("ts_type"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="is_named",
                value=row.get("is_named"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="has_error",
                value=row.get("has_error"),
            )

    if ts_errors is not None and ts_errors.num_rows:
        for row in ts_errors.to_pylist():
            node_id = _row_id(row, "ts_error_id")
            if node_id is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="node_kind",
                value=NodeKind.TS_ERROR.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="ts_type",
                value=row.get("ts_type"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="is_error",
                value=row.get("is_error"),
            )

    if ts_missing is not None and ts_missing.num_rows:
        for row in ts_missing.to_pylist():
            node_id = _row_id(row, "ts_missing_id")
            if node_id is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="node_kind",
                value=NodeKind.TS_MISSING.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="ts_type",
                value=row.get("ts_type"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id,
                key="is_missing",
                value=row.get("is_missing"),
            )


def _add_type_props(
    rows: list[PropRow],
    type_exprs_norm: pa.Table | None,
    types_norm: pa.Table | None,
) -> None:
    if type_exprs_norm is not None and type_exprs_norm.num_rows:
        for row in type_exprs_norm.to_pylist():
            type_expr_id = _row_id(row, "type_expr_id")
            if type_expr_id is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id,
                key="node_kind",
                value=NodeKind.TYPE_EXPR.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id,
                key="expr_text",
                value=row.get("expr_text"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id,
                key="expr_kind",
                value=row.get("expr_kind"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id,
                key="expr_role",
                value=row.get("expr_role"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id,
                key="param_name",
                value=row.get("param_name"),
            )

    if types_norm is not None and types_norm.num_rows:
        for row in types_norm.to_pylist():
            type_id = _row_id(row, "type_id")
            if type_id is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_id,
                key="node_kind",
                value=NodeKind.TYPE.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_id,
                key="type_repr",
                value=row.get("type_repr"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_id,
                key="type_form",
                value=row.get("type_form"),
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_id,
                key="origin",
                value=row.get("origin"),
            )


def _add_diag_props(rows: list[PropRow], diagnostics_norm: pa.Table | None) -> None:
    if diagnostics_norm is None or diagnostics_norm.num_rows == 0:
        return
    for row in diagnostics_norm.to_pylist():
        diag_id = _row_id(row, "diag_id")
        if diag_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id,
            key="node_kind",
            value=NodeKind.DIAG.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id,
            key="severity",
            value=row.get("severity"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id,
            key="message",
            value=row.get("message"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id,
            key="diag_source",
            value=row.get("diag_source"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id,
            key="code",
            value=row.get("code"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id,
            key="details_json",
            value=row.get("details_json"),
        )


def _add_runtime_props(
    rows: list[PropRow],
    rt_objects: pa.Table | None,
    rt_signatures: pa.Table | None,
    rt_signature_params: pa.Table | None,
    rt_members: pa.Table | None,
) -> None:
    if rt_objects is not None and rt_objects.num_rows:
        _add_runtime_object_props(rows, rt_objects)

    if rt_signatures is not None and rt_signatures.num_rows:
        _add_runtime_signature_props(rows, rt_signatures)

    if rt_signature_params is not None and rt_signature_params.num_rows:
        _add_runtime_param_props(rows, rt_signature_params)

    if rt_members is not None and rt_members.num_rows:
        _add_runtime_member_props(rows, rt_members)


def _add_runtime_object_props(rows: list[PropRow], rt_objects: pa.Table) -> None:
    for row in rt_objects.to_pylist():
        rt_id = _row_id(row, "rt_id")
        if rt_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id,
            key="node_kind",
            value=NodeKind.RT_OBJECT.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id,
            key="module",
            value=row.get("module"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id,
            key="qualname",
            value=row.get("qualname"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id,
            key="name",
            value=row.get("name"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id,
            key="obj_type",
            value=row.get("obj_type"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id,
            key="source_path",
            value=row.get("source_path"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id,
            key="source_line",
            value=row.get("source_line"),
        )


def _add_runtime_signature_props(rows: list[PropRow], rt_signatures: pa.Table) -> None:
    for row in rt_signatures.to_pylist():
        sig_id = _row_id(row, "sig_id")
        if sig_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=sig_id,
            key="node_kind",
            value=NodeKind.RT_SIGNATURE.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=sig_id,
            key="signature",
            value=row.get("signature"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=sig_id,
            key="return_annotation",
            value=row.get("return_annotation"),
        )


def _add_runtime_param_props(rows: list[PropRow], rt_signature_params: pa.Table) -> None:
    for row in rt_signature_params.to_pylist():
        param_id = _row_id(row, "param_id")
        if param_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id,
            key="node_kind",
            value=NodeKind.RT_SIGNATURE_PARAM.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id,
            key="name",
            value=row.get("name"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id,
            key="kind",
            value=row.get("kind"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id,
            key="default_repr",
            value=row.get("default_repr"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id,
            key="annotation_repr",
            value=row.get("annotation_repr"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id,
            key="position",
            value=row.get("position"),
        )


def _add_runtime_member_props(rows: list[PropRow], rt_members: pa.Table) -> None:
    for row in rt_members.to_pylist():
        member_id = _row_id(row, "member_id")
        if member_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id,
            key="node_kind",
            value=NodeKind.RT_MEMBER.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id,
            key="name",
            value=row.get("name"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id,
            key="member_kind",
            value=row.get("member_kind"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id,
            key="value_repr",
            value=row.get("value_repr"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id,
            key="value_module",
            value=row.get("value_module"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id,
            key="value_qualname",
            value=row.get("value_qualname"),
        )


def _add_edge_props(rows: list[PropRow], cpg_edges: pa.Table | None) -> None:
    if cpg_edges is None or cpg_edges.num_rows == 0:
        return
    for row in cpg_edges.to_pylist():
        edge_id = _row_id(row, "edge_id")
        if edge_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="edge_kind",
            value=row.get("edge_kind"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="origin",
            value=row.get("origin"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="resolution_method",
            value=row.get("resolution_method"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="confidence",
            value=row.get("confidence"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="score",
            value=row.get("score"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="symbol_roles",
            value=row.get("symbol_roles"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="qname_source",
            value=row.get("qname_source"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="ambiguity_group_id",
            value=row.get("ambiguity_group_id"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="rule_name",
            value=row.get("rule_name"),
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id,
            key="rule_priority",
            value=row.get("rule_priority"),
        )


def build_cpg_props_raw(
    *,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
) -> pa.Table:
    """Build CPG properties without finalization.

    Returns
    -------
    pyarrow.Table
        Raw properties table.
    """
    options = options or PropsBuildOptions()
    tables = inputs or PropsInputTables()
    rows: list[PropRow] = []

    if options.include_node_props:
        _add_file_props(rows, tables.repo_files)
        _add_name_ref_props(rows, tables.cst_name_refs)
        _add_import_props(rows, tables.cst_imports)
        _add_callsite_props(
            rows, tables.cst_callsites, include_heavy_json=options.include_heavy_json_props
        )
        _add_def_props(rows, tables.cst_defs, include_heavy_json=options.include_heavy_json_props)
        _add_qname_props(rows, tables.dim_qualified_names)
        _add_symbol_props(
            rows,
            tables.scip_symbol_information,
            include_heavy_json=options.include_heavy_json_props,
        )
        _add_ts_props(rows, tables.ts_nodes, tables.ts_errors, tables.ts_missing)
        _add_type_props(rows, tables.type_exprs_norm, tables.types_norm)
        _add_diag_props(rows, tables.diagnostics_norm)
        _add_runtime_props(
            rows,
            tables.rt_objects,
            tables.rt_signatures,
            tables.rt_signature_params,
            tables.rt_members,
        )

    if options.include_edge_props:
        _add_edge_props(rows, tables.cpg_edges)

    if not rows:
        return empty_table(CPG_PROPS_SCHEMA)

    return pa.Table.from_pylist(rows, schema=CPG_PROPS_SCHEMA)


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
