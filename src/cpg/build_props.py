"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.empty import empty_table
from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.iter import iter_arrays
from arrowdsl.runtime import ExecutionContext
from cpg.kinds import (
    SCIP_ROLE_FORWARD_DEFINITION,
    SCIP_ROLE_GENERATED,
    SCIP_ROLE_TEST,
    EntityKind,
    NodeKind,
)
from cpg.schemas import CPG_PROPS_CONTRACT, CPG_PROPS_SCHEMA, SCHEMA_VERSION

type PropValue = object | None
type PropRow = dict[str, object]
type ArrayLike = pa.Array | pa.ChunkedArray
QNAME_LIST_TYPE = pa.list_(pa.struct([("name", pa.string()), ("source", pa.string())]))
DIAG_DETAIL_STRUCT = pa.struct(
    [
        ("detail_kind", pa.string()),
        ("error_type", pa.string()),
        ("source", pa.string()),
        ("tags", pa.list_(pa.string())),
    ]
)
DIAG_DETAILS_TYPE = pa.list_(DIAG_DETAIL_STRUCT)


def _row_id(value: object | None) -> str | None:
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


def _column_or_null(table: pa.Table, col: str, dtype: pa.DataType) -> ArrayLike:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)


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
    scip_occurrences: pa.Table | None = None
    scip_external_symbol_information: pa.Table | None = None
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
    arrays = [
        _column_or_null(repo_files, "file_id", pa.string()),
        _column_or_null(repo_files, "path", pa.string()),
        _column_or_null(repo_files, "size_bytes", pa.int64()),
        _column_or_null(repo_files, "file_sha256", pa.string()),
        _column_or_null(repo_files, "encoding", pa.string()),
    ]
    for file_id, path, size_bytes, file_sha256, encoding in iter_arrays(arrays):
        file_id_value = _row_id(file_id)
        if file_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id_value,
            key="node_kind",
            value=NodeKind.PY_FILE.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id_value,
            key="path",
            value=path,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id_value,
            key="size_bytes",
            value=size_bytes,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id_value,
            key="file_sha256",
            value=file_sha256,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=file_id_value,
            key="encoding",
            value=encoding,
        )


def _add_name_ref_props(rows: list[PropRow], cst_name_refs: pa.Table | None) -> None:
    if cst_name_refs is None or cst_name_refs.num_rows == 0:
        return
    arrays = [
        _column_or_null(cst_name_refs, "name_ref_id", pa.string()),
        _column_or_null(cst_name_refs, "name", pa.string()),
        _column_or_null(cst_name_refs, "expr_ctx", pa.string()),
    ]
    for name_id, name, expr_ctx in iter_arrays(arrays):
        name_id_value = _row_id(name_id)
        if name_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=name_id_value,
            key="node_kind",
            value=NodeKind.CST_NAME_REF.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=name_id_value,
            key="name",
            value=name,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=name_id_value,
            key="expr_context",
            value=_expr_context_value(expr_ctx),
        )


def _add_import_props(rows: list[PropRow], cst_imports: pa.Table | None) -> None:
    if cst_imports is None or cst_imports.num_rows == 0:
        return
    id_key = "import_alias_id" if "import_alias_id" in cst_imports.column_names else "import_id"
    arrays = [
        _column_or_null(cst_imports, id_key, pa.string()),
        _column_or_null(cst_imports, "kind", pa.string()),
        _column_or_null(cst_imports, "module", pa.string()),
        _column_or_null(cst_imports, "relative_level", pa.int64()),
        _column_or_null(cst_imports, "name", pa.string()),
        _column_or_null(cst_imports, "asname", pa.string()),
        _column_or_null(cst_imports, "is_star", pa.bool_()),
    ]
    for (
        import_id,
        kind,
        module,
        relative_level,
        name,
        asname,
        is_star,
    ) in iter_arrays(arrays):
        import_id_value = _row_id(import_id)
        if import_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id_value,
            key="node_kind",
            value=NodeKind.CST_IMPORT_ALIAS.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id_value,
            key="import_kind",
            value=kind,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id_value,
            key="module",
            value=module,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id_value,
            key="relative_level",
            value=relative_level,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id_value,
            key="imported_name",
            value=name,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id_value,
            key="name",
            value=name,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id_value,
            key="asname",
            value=asname,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=import_id_value,
            key="is_star",
            value=is_star,
        )


def _add_callsite_props(
    rows: list[PropRow],
    cst_callsites: pa.Table | None,
    *,
    include_heavy_json: bool,
) -> None:
    if cst_callsites is None or cst_callsites.num_rows == 0:
        return
    arrays = [
        _column_or_null(cst_callsites, "call_id", pa.string()),
        _column_or_null(cst_callsites, "callee_shape", pa.string()),
        _column_or_null(cst_callsites, "callee_text", pa.string()),
        _column_or_null(cst_callsites, "callee_dotted", pa.string()),
        _column_or_null(cst_callsites, "arg_count", pa.int64()),
    ]
    if include_heavy_json:
        arrays.append(_column_or_null(cst_callsites, "callee_qnames", QNAME_LIST_TYPE))
    for values in iter_arrays(arrays):
        if include_heavy_json:
            call_id, callee_shape, callee_text, callee_dotted, arg_count, callee_qnames = values
        else:
            call_id, callee_shape, callee_text, callee_dotted, arg_count = values
            callee_qnames = None
        call_id_value = _row_id(call_id)
        if call_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id_value,
            key="node_kind",
            value=NodeKind.CST_CALLSITE.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id_value,
            key="callee_shape",
            value=callee_shape,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id_value,
            key="callee_text",
            value=callee_text,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id_value,
            key="callee_dotted",
            value=callee_dotted,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=call_id_value,
            key="arg_count",
            value=arg_count,
        )
        if include_heavy_json:
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=call_id_value,
                key="callee_qnames",
                value=callee_qnames,
            )


def _add_def_props(
    rows: list[PropRow],
    cst_defs: pa.Table | None,
    *,
    include_heavy_json: bool,
) -> None:
    if cst_defs is None or cst_defs.num_rows == 0:
        return
    arrays = [
        _column_or_null(cst_defs, "def_id", pa.string()),
        _column_or_null(cst_defs, "def_kind", pa.string()),
        _column_or_null(cst_defs, "kind", pa.string()),
        _column_or_null(cst_defs, "name", pa.string()),
        _column_or_null(cst_defs, "container_def_id", pa.string()),
    ]
    if include_heavy_json:
        arrays.append(_column_or_null(cst_defs, "qnames", QNAME_LIST_TYPE))
    for values in iter_arrays(arrays):
        if include_heavy_json:
            def_id, def_kind, kind, name, container_def_id, qnames = values
        else:
            def_id, def_kind, kind, name, container_def_id = values
            qnames = None
        def_id_value = _row_id(def_id)
        if def_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=def_id_value,
            key="node_kind",
            value=NodeKind.CST_DEF.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=def_id_value,
            key="def_kind",
            value=def_kind or kind,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=def_id_value,
            key="name",
            value=name,
        )
        if include_heavy_json:
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=def_id_value,
                key="qnames",
                value=qnames,
            )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=def_id_value,
            key="container_def_id",
            value=container_def_id,
        )


def _add_qname_props(rows: list[PropRow], dim_qualified_names: pa.Table | None) -> None:
    if (
        dim_qualified_names is None
        or dim_qualified_names.num_rows == 0
        or "qname_id" not in dim_qualified_names.column_names
    ):
        return
    arrays = [
        _column_or_null(dim_qualified_names, "qname_id", pa.string()),
        _column_or_null(dim_qualified_names, "qname", pa.string()),
    ]
    for qname_id, qname in iter_arrays(arrays):
        qname_id_value = _row_id(qname_id)
        if qname_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=qname_id_value,
            key="node_kind",
            value=NodeKind.PY_QUALIFIED_NAME.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=qname_id_value,
            key="qname",
            value=qname,
        )


def _add_symbol_props(
    rows: list[PropRow],
    scip_symbol_information: pa.Table | None,
    *,
    include_heavy_json: bool,
) -> None:
    if scip_symbol_information is None or scip_symbol_information.num_rows == 0:
        return
    arrays = [
        _column_or_null(scip_symbol_information, "symbol", pa.string()),
        _column_or_null(scip_symbol_information, "display_name", pa.string()),
        _column_or_null(scip_symbol_information, "kind", pa.string()),
        _column_or_null(scip_symbol_information, "enclosing_symbol", pa.string()),
    ]
    if include_heavy_json:
        arrays.append(
            _column_or_null(scip_symbol_information, "documentation", pa.list_(pa.string()))
        )
        arrays.append(
            _column_or_null(scip_symbol_information, "signature_documentation", pa.string())
        )
    for values in iter_arrays(arrays):
        if include_heavy_json:
            (
                symbol,
                display_name,
                kind,
                enclosing_symbol,
                documentation,
                signature_documentation,
            ) = values
        else:
            symbol, display_name, kind, enclosing_symbol = values
            documentation = None
            signature_documentation = None
        symbol_id = _row_id(symbol)
        if symbol_id is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol_id,
            key="node_kind",
            value=NodeKind.SCIP_SYMBOL.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol_id,
            key="symbol",
            value=symbol,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol_id,
            key="display_name",
            value=display_name,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol_id,
            key="symbol_kind",
            value=kind,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=symbol_id,
            key="enclosing_symbol",
            value=enclosing_symbol,
        )
        if include_heavy_json:
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=symbol_id,
                key="documentation",
                value=documentation,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=symbol_id,
                key="signature_documentation",
                value=signature_documentation,
            )


def _add_scip_role_props(rows: list[PropRow], scip_occurrences: pa.Table | None) -> None:
    """Add role-derived properties for SCIP symbols."""
    if (
        scip_occurrences is None
        or scip_occurrences.num_rows == 0
        or "symbol" not in scip_occurrences.column_names
        or "symbol_roles" not in scip_occurrences.column_names
    ):
        return

    symbols = pc.cast(_column_or_null(scip_occurrences, "symbol", pa.string()), pa.string())
    roles = pc.cast(_column_or_null(scip_occurrences, "symbol_roles", pa.int64()), pa.int64())
    flag_arrays: list[pa.Array | pa.ChunkedArray] = []
    flag_names: list[str] = []
    for name, mask, _ in _ROLE_FLAG_SPECS:
        flag = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(mask)), pa.scalar(0))
        flag_arrays.append(pc.cast(flag, pa.int32()))
        flag_names.append(name)

    flags_table = pa.Table.from_arrays([symbols, *flag_arrays], names=["symbol", *flag_names])
    aggs = [(name, "max") for name in flag_names]
    aggregated = flags_table.group_by(["symbol"], use_threads=True).aggregate(aggs)
    aggregated = aggregated.rename_columns(["symbol", *flag_names])
    arrays = [aggregated["symbol"], *[aggregated[name] for name in flag_names]]
    for values in iter_arrays(arrays):
        symbol = values[0]
        if not isinstance(symbol, str) or not symbol:
            continue
        flags: set[str] = set()
        for name, raw in zip(flag_names, values[1:], strict=True):
            if isinstance(raw, bool):
                value = int(raw)
            elif isinstance(raw, int):
                value = raw
            else:
                value = 0
            if value == 1:
                flags.add(name)
        if flags:
            _emit_scip_role_props(rows, symbol, flags)


_ROLE_FLAG_SPECS: tuple[tuple[str, int, str], ...] = (
    ("generated", SCIP_ROLE_GENERATED, "scip_role_generated"),
    ("test", SCIP_ROLE_TEST, "scip_role_test"),
    ("forward_definition", SCIP_ROLE_FORWARD_DEFINITION, "scip_role_forward_definition"),
)


def _emit_scip_role_props(rows: list[PropRow], symbol: str, flags: set[str]) -> None:
    for name, _, prop_key in _ROLE_FLAG_SPECS:
        if name in flags:
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=symbol,
                key=prop_key,
                value=True,
            )


def _add_ts_props(
    rows: list[PropRow],
    ts_nodes: pa.Table | None,
    ts_errors: pa.Table | None,
    ts_missing: pa.Table | None,
) -> None:
    if ts_nodes is not None and ts_nodes.num_rows:
        arrays = [
            _column_or_null(ts_nodes, "ts_node_id", pa.string()),
            _column_or_null(ts_nodes, "ts_type", pa.string()),
            _column_or_null(ts_nodes, "is_named", pa.bool_()),
            _column_or_null(ts_nodes, "has_error", pa.bool_()),
        ]
        for node_id, ts_type, is_named, has_error in iter_arrays(arrays):
            node_id_value = _row_id(node_id)
            if node_id_value is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="node_kind",
                value=NodeKind.TS_NODE.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="ts_type",
                value=ts_type,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="is_named",
                value=is_named,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="has_error",
                value=has_error,
            )

    if ts_errors is not None and ts_errors.num_rows:
        arrays = [
            _column_or_null(ts_errors, "ts_error_id", pa.string()),
            _column_or_null(ts_errors, "ts_type", pa.string()),
            _column_or_null(ts_errors, "is_error", pa.bool_()),
        ]
        for node_id, ts_type, is_error in iter_arrays(arrays):
            node_id_value = _row_id(node_id)
            if node_id_value is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="node_kind",
                value=NodeKind.TS_ERROR.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="ts_type",
                value=ts_type,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="is_error",
                value=is_error,
            )

    if ts_missing is not None and ts_missing.num_rows:
        arrays = [
            _column_or_null(ts_missing, "ts_missing_id", pa.string()),
            _column_or_null(ts_missing, "ts_type", pa.string()),
            _column_or_null(ts_missing, "is_missing", pa.bool_()),
        ]
        for node_id, ts_type, is_missing in iter_arrays(arrays):
            node_id_value = _row_id(node_id)
            if node_id_value is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="node_kind",
                value=NodeKind.TS_MISSING.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="ts_type",
                value=ts_type,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=node_id_value,
                key="is_missing",
                value=is_missing,
            )


def _add_type_props(
    rows: list[PropRow],
    type_exprs_norm: pa.Table | None,
    types_norm: pa.Table | None,
) -> None:
    if type_exprs_norm is not None and type_exprs_norm.num_rows:
        arrays = [
            _column_or_null(type_exprs_norm, "type_expr_id", pa.string()),
            _column_or_null(type_exprs_norm, "expr_text", pa.string()),
            _column_or_null(type_exprs_norm, "expr_kind", pa.string()),
            _column_or_null(type_exprs_norm, "expr_role", pa.string()),
            _column_or_null(type_exprs_norm, "param_name", pa.string()),
        ]
        for type_expr_id, expr_text, expr_kind, expr_role, param_name in iter_arrays(arrays):
            type_expr_id_value = _row_id(type_expr_id)
            if type_expr_id_value is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id_value,
                key="node_kind",
                value=NodeKind.TYPE_EXPR.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id_value,
                key="expr_text",
                value=expr_text,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id_value,
                key="expr_kind",
                value=expr_kind,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id_value,
                key="expr_role",
                value=expr_role,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_expr_id_value,
                key="param_name",
                value=param_name,
            )

    if types_norm is not None and types_norm.num_rows:
        arrays = [
            _column_or_null(types_norm, "type_id", pa.string()),
            _column_or_null(types_norm, "type_repr", pa.string()),
            _column_or_null(types_norm, "type_form", pa.string()),
            _column_or_null(types_norm, "origin", pa.string()),
        ]
        for type_id, type_repr, type_form, origin in iter_arrays(arrays):
            type_id_value = _row_id(type_id)
            if type_id_value is None:
                continue
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_id_value,
                key="node_kind",
                value=NodeKind.TYPE.value,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_id_value,
                key="type_repr",
                value=type_repr,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_id_value,
                key="type_form",
                value=type_form,
            )
            _add_prop(
                rows,
                entity_kind=EntityKind.NODE,
                entity_id=type_id_value,
                key="origin",
                value=origin,
            )


def _add_diag_props(rows: list[PropRow], diagnostics_norm: pa.Table | None) -> None:
    if diagnostics_norm is None or diagnostics_norm.num_rows == 0:
        return
    arrays = [
        _column_or_null(diagnostics_norm, "diag_id", pa.string()),
        _column_or_null(diagnostics_norm, "severity", pa.string()),
        _column_or_null(diagnostics_norm, "message", pa.string()),
        _column_or_null(diagnostics_norm, "diag_source", pa.string()),
        _column_or_null(diagnostics_norm, "code", pa.string()),
        _column_or_null(diagnostics_norm, "details", DIAG_DETAILS_TYPE),
    ]
    for diag_id, severity, message, diag_source, code, details in iter_arrays(arrays):
        diag_id_value = _row_id(diag_id)
        if diag_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id_value,
            key="node_kind",
            value=NodeKind.DIAG.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id_value,
            key="severity",
            value=severity,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id_value,
            key="message",
            value=message,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id_value,
            key="diag_source",
            value=diag_source,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id_value,
            key="code",
            value=code,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=diag_id_value,
            key="details",
            value=details,
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
    arrays = [
        _column_or_null(rt_objects, "rt_id", pa.string()),
        _column_or_null(rt_objects, "module", pa.string()),
        _column_or_null(rt_objects, "qualname", pa.string()),
        _column_or_null(rt_objects, "name", pa.string()),
        _column_or_null(rt_objects, "obj_type", pa.string()),
        _column_or_null(rt_objects, "source_path", pa.string()),
        _column_or_null(rt_objects, "source_line", pa.int64()),
    ]
    for (
        rt_id,
        module,
        qualname,
        name,
        obj_type,
        source_path,
        source_line,
    ) in iter_arrays(arrays):
        rt_id_value = _row_id(rt_id)
        if rt_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id_value,
            key="node_kind",
            value=NodeKind.RT_OBJECT.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id_value,
            key="module",
            value=module,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id_value,
            key="qualname",
            value=qualname,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id_value,
            key="name",
            value=name,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id_value,
            key="obj_type",
            value=obj_type,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id_value,
            key="source_path",
            value=source_path,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=rt_id_value,
            key="source_line",
            value=source_line,
        )


def _add_runtime_signature_props(rows: list[PropRow], rt_signatures: pa.Table) -> None:
    arrays = [
        _column_or_null(rt_signatures, "sig_id", pa.string()),
        _column_or_null(rt_signatures, "signature", pa.string()),
        _column_or_null(rt_signatures, "return_annotation", pa.string()),
    ]
    for sig_id, signature, return_annotation in iter_arrays(arrays):
        sig_id_value = _row_id(sig_id)
        if sig_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=sig_id_value,
            key="node_kind",
            value=NodeKind.RT_SIGNATURE.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=sig_id_value,
            key="signature",
            value=signature,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=sig_id_value,
            key="return_annotation",
            value=return_annotation,
        )


def _add_runtime_param_props(rows: list[PropRow], rt_signature_params: pa.Table) -> None:
    arrays = [
        _column_or_null(rt_signature_params, "param_id", pa.string()),
        _column_or_null(rt_signature_params, "name", pa.string()),
        _column_or_null(rt_signature_params, "kind", pa.string()),
        _column_or_null(rt_signature_params, "default_repr", pa.string()),
        _column_or_null(rt_signature_params, "annotation_repr", pa.string()),
        _column_or_null(rt_signature_params, "position", pa.int64()),
    ]
    for (
        param_id,
        name,
        kind,
        default_repr,
        annotation_repr,
        position,
    ) in iter_arrays(arrays):
        param_id_value = _row_id(param_id)
        if param_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id_value,
            key="node_kind",
            value=NodeKind.RT_SIGNATURE_PARAM.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id_value,
            key="name",
            value=name,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id_value,
            key="kind",
            value=kind,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id_value,
            key="default_repr",
            value=default_repr,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id_value,
            key="annotation_repr",
            value=annotation_repr,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=param_id_value,
            key="position",
            value=position,
        )


def _add_runtime_member_props(rows: list[PropRow], rt_members: pa.Table) -> None:
    arrays = [
        _column_or_null(rt_members, "member_id", pa.string()),
        _column_or_null(rt_members, "name", pa.string()),
        _column_or_null(rt_members, "member_kind", pa.string()),
        _column_or_null(rt_members, "value_repr", pa.string()),
        _column_or_null(rt_members, "value_module", pa.string()),
        _column_or_null(rt_members, "value_qualname", pa.string()),
    ]
    for (
        member_id,
        name,
        member_kind,
        value_repr,
        value_module,
        value_qualname,
    ) in iter_arrays(arrays):
        member_id_value = _row_id(member_id)
        if member_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id_value,
            key="node_kind",
            value=NodeKind.RT_MEMBER.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id_value,
            key="name",
            value=name,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id_value,
            key="member_kind",
            value=member_kind,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id_value,
            key="value_repr",
            value=value_repr,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id_value,
            key="value_module",
            value=value_module,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=member_id_value,
            key="value_qualname",
            value=value_qualname,
        )


def _add_edge_props(rows: list[PropRow], cpg_edges: pa.Table | None) -> None:
    if cpg_edges is None or cpg_edges.num_rows == 0:
        return
    arrays = [
        _column_or_null(cpg_edges, "edge_id", pa.string()),
        _column_or_null(cpg_edges, "edge_kind", pa.string()),
        _column_or_null(cpg_edges, "origin", pa.string()),
        _column_or_null(cpg_edges, "resolution_method", pa.string()),
        _column_or_null(cpg_edges, "confidence", pa.float64()),
        _column_or_null(cpg_edges, "score", pa.float64()),
        _column_or_null(cpg_edges, "symbol_roles", pa.int64()),
        _column_or_null(cpg_edges, "qname_source", pa.string()),
        _column_or_null(cpg_edges, "ambiguity_group_id", pa.string()),
        _column_or_null(cpg_edges, "rule_name", pa.string()),
        _column_or_null(cpg_edges, "rule_priority", pa.int64()),
    ]
    for (
        edge_id,
        edge_kind,
        origin,
        resolution_method,
        confidence,
        score,
        symbol_roles,
        qname_source,
        ambiguity_group_id,
        rule_name,
        rule_priority,
    ) in iter_arrays(arrays):
        edge_id_value = _row_id(edge_id)
        if edge_id_value is None:
            continue
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="edge_kind",
            value=edge_kind,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="origin",
            value=origin,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="resolution_method",
            value=resolution_method,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="confidence",
            value=confidence,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="score",
            value=score,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="symbol_roles",
            value=symbol_roles,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="qname_source",
            value=qname_source,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="ambiguity_group_id",
            value=ambiguity_group_id,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="rule_name",
            value=rule_name,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.EDGE,
            entity_id=edge_id_value,
            key="rule_priority",
            value=rule_priority,
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
        _add_symbol_props(
            rows,
            tables.scip_external_symbol_information,
            include_heavy_json=options.include_heavy_json_props,
        )
        _add_scip_role_props(rows, tables.scip_occurrences)
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
