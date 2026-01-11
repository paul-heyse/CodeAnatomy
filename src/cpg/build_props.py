"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

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


def _empty() -> pa.Table:
    return pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in CPG_PROPS_SCHEMA],
        schema=CPG_PROPS_SCHEMA,
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

    repo_files: pa.Table | None = None
    cst_name_refs: pa.Table | None = None
    cst_imports: pa.Table | None = None
    cst_callsites: pa.Table | None = None
    cst_defs: pa.Table | None = None
    dim_qualified_names: pa.Table | None = None
    scip_symbol_information: pa.Table | None = None
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
            value=NodeKind.PY_NAME_REF.value,
        )
        _add_prop(
            rows, entity_kind=EntityKind.NODE, entity_id=name_id, key="name", value=row.get("name")
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=name_id,
            key="expr_ctx",
            value=row.get("expr_ctx"),
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
            value=NodeKind.PY_IMPORT_ALIAS.value,
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
            value=NodeKind.PY_CALLSITE.value,
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
            value=NodeKind.PY_DEF.value,
        )
        _add_prop(
            rows,
            entity_kind=EntityKind.NODE,
            entity_id=def_id,
            key="def_kind",
            value=row.get("kind"),
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
            value=NodeKind.PY_SYMBOL.value,
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

    if options.include_edge_props:
        _add_edge_props(rows, tables.cpg_edges)

    if not rows:
        return _empty()

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
