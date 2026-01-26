"""Incremental impact closure helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import ibis
import pyarrow as pa

from arrowdsl.core.array_iter import iter_table_rows
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.schema import align_table, empty_table
from ibis_engine.sources import read_delta_ibis
from incremental.ibis_exec import ibis_expr_to_table
from incremental.ibis_utils import ibis_table_from_arrow
from incremental.registry_specs import dataset_schema
from incremental.runtime import IncrementalRuntime
from incremental.types import IncrementalFileChanges

_EXPORT_KEY_SCHEMA = pa.schema(
    [
        pa.field("module_fqn", pa.string()),
        pa.field("name", pa.string()),
        pa.field("qname", pa.string()),
    ]
)


def impacted_callers_from_changed_exports(
    *,
    runtime: IncrementalRuntime,
    changed_exports: TableLike,
    prev_rel_callsite_qname: str | None,
    prev_rel_callsite_symbol: str | None,
) -> pa.Table:
    """Return caller impact rows from changed exports.

    Returns
    -------
    pa.Table
        Impacted callers table derived from callsite relationships.
    """
    backend = runtime.ibis_backend()
    schema = dataset_schema("inc_impacted_callers_v1")
    changed = cast("pa.Table", changed_exports)
    if changed.num_rows == 0:
        return empty_table(schema)

    pieces: list[ibis.Table] = []
    changed_table = ibis_table_from_arrow(backend, changed)
    if prev_rel_callsite_qname is not None:
        rel_qname = read_delta_ibis(backend, prev_rel_callsite_qname)
        changed_qname = changed_table.filter(changed_table.qname_id.notnull())
        qname_hits = rel_qname.inner_join(
            changed_qname,
            predicates=[rel_qname.qname_id == changed_qname.qname_id],
        )
        qname_hits = qname_hits.select(
            file_id=rel_qname.edge_owner_file_id,
            path=rel_qname.path,
            reason_kind=ibis.literal("callsite_qname"),
            reason_ref=rel_qname.qname_id,
        )
        pieces.append(qname_hits)

    if prev_rel_callsite_symbol is not None:
        rel_symbol = read_delta_ibis(backend, prev_rel_callsite_symbol)
        changed_symbol = changed_table.filter(changed_table.symbol.notnull())
        symbol_hits = rel_symbol.inner_join(
            changed_symbol,
            predicates=[rel_symbol.symbol == changed_symbol.symbol],
        )
        symbol_hits = symbol_hits.select(
            file_id=rel_symbol.edge_owner_file_id,
            path=rel_symbol.path,
            reason_kind=ibis.literal("callsite_symbol"),
            reason_ref=rel_symbol.symbol,
        )
        pieces.append(symbol_hits)

    if not pieces:
        return empty_table(schema)
    combined = _union_all_tables(pieces).distinct()
    result = ibis_expr_to_table(
        combined,
        runtime=runtime,
        name="impacted_callers",
    )
    return align_table(result, schema=schema, safe_cast=True)


def impacted_importers_from_changed_exports(
    *,
    runtime: IncrementalRuntime,
    changed_exports: TableLike,
    prev_imports_resolved: str | None,
) -> pa.Table:
    """Return importer impact rows from changed exports.

    Returns
    -------
    pa.Table
        Impacted importers table derived from resolved imports.
    """
    backend = runtime.ibis_backend()
    schema = dataset_schema("inc_impacted_importers_v1")
    if prev_imports_resolved is None:
        return empty_table(schema)
    changed = cast("pa.Table", changed_exports)
    if changed.num_rows == 0:
        return empty_table(schema)
    exports = _export_import_keys(changed)
    if exports.num_rows == 0:
        return empty_table(schema)

    imports_resolved = read_delta_ibis(backend, prev_imports_resolved)
    exports_table = ibis_table_from_arrow(backend, exports)
    by_name = imports_resolved.inner_join(
        exports_table,
        predicates=[
            imports_resolved.imported_module_fqn == exports_table.module_fqn,
            imports_resolved.imported_name == exports_table.name,
        ],
    )
    by_name = by_name.select(
        file_id=imports_resolved.importer_file_id,
        path=imports_resolved.importer_path,
        reason_kind=ibis.literal("import_name"),
        reason_ref=exports_table.qname,
    )

    star = imports_resolved.filter(imports_resolved.is_star == ibis.literal(value=True))
    by_star = star.inner_join(
        exports_table,
        predicates=[star.imported_module_fqn == exports_table.module_fqn],
    )
    by_star = by_star.select(
        file_id=star.importer_file_id,
        path=star.importer_path,
        reason_kind=ibis.literal("import_star"),
        reason_ref=exports_table.qname,
    )

    combined = _union_all_tables([by_name, by_star]).distinct()
    result = ibis_expr_to_table(
        combined,
        runtime=runtime,
        name="impacted_importers",
    )
    return align_table(result, schema=schema, safe_cast=True)


def import_closure_only_from_changed_exports(
    *,
    runtime: IncrementalRuntime,
    changed_exports: TableLike,
    prev_imports_resolved: str | None,
) -> pa.Table:
    """Return module-level import closures for changed exports.

    Returns
    -------
    pa.Table
        Impacted importers table for module-level imports.
    """
    backend = runtime.ibis_backend()
    schema = dataset_schema("inc_impacted_importers_v1")
    if prev_imports_resolved is None:
        return empty_table(schema)
    changed = cast("pa.Table", changed_exports)
    if changed.num_rows == 0:
        return empty_table(schema)
    exports = _export_import_keys(changed)
    if exports.num_rows == 0:
        return empty_table(schema)

    imports_resolved = read_delta_ibis(backend, prev_imports_resolved)
    exports_table = ibis_table_from_arrow(backend, exports)
    module_imports = imports_resolved.filter(
        imports_resolved.imported_name.isnull()
        & (imports_resolved.is_star == ibis.literal(value=False))
    )
    module_hits = module_imports.inner_join(
        exports_table,
        predicates=[module_imports.imported_module_fqn == exports_table.module_fqn],
    )
    module_hits = module_hits.select(
        file_id=module_imports.importer_file_id,
        path=module_imports.importer_path,
        reason_kind=ibis.literal("import_module"),
        reason_ref=exports_table.qname,
    )

    star = imports_resolved.filter(imports_resolved.is_star == ibis.literal(value=True))
    star_hits = star.inner_join(
        exports_table,
        predicates=[star.imported_module_fqn == exports_table.module_fqn],
    )
    star_hits = star_hits.select(
        file_id=star.importer_file_id,
        path=star.importer_path,
        reason_kind=ibis.literal("import_star"),
        reason_ref=exports_table.qname,
    )

    combined = _union_all_tables([module_hits, star_hits]).distinct()
    result = ibis_expr_to_table(
        combined,
        runtime=runtime,
        name="import_closure_only",
    )
    return align_table(result, schema=schema, safe_cast=True)


@dataclass(frozen=True)
class ImpactedFileInputs:
    """Inputs required to merge impacted file tables."""

    changed_files: TableLike
    callers: TableLike | None
    importers: TableLike | None
    import_closure_only: TableLike | None


def merge_impacted_files(
    runtime: IncrementalRuntime,
    inputs: ImpactedFileInputs,
    *,
    strategy: str,
) -> pa.Table:
    """Merge impacted file tables using the chosen strategy.

    Returns
    -------
    pa.Table
        Impacted files table aligned to ``inc_impacted_files_v2``.
    """
    backend = runtime.ibis_backend()
    schema = dataset_schema("inc_impacted_files_v2")
    tables: list[pa.Table] = [align_table(cast("pa.Table", inputs.changed_files), schema=schema)]

    if strategy == "symbol_closure":
        tables.extend(_optional_tables([inputs.callers, inputs.importers], schema=schema))
    elif strategy == "import_closure":
        tables.extend(_optional_tables([inputs.import_closure_only], schema=schema))
    else:
        tables.extend(
            _optional_tables(
                [inputs.callers, inputs.importers, inputs.import_closure_only],
                schema=schema,
            )
        )

    ibis_tables = [ibis_table_from_arrow(backend, table) for table in tables if table.num_rows > 0]
    if not ibis_tables:
        return empty_table(schema)
    combined = _union_all_tables(ibis_tables).distinct()
    result = ibis_expr_to_table(
        combined,
        runtime=runtime,
        name="impacted_files",
    )
    return align_table(result, schema=schema, safe_cast=True)


def changed_file_impact_table(
    *,
    file_changes: IncrementalFileChanges,
    incremental_diff: pa.Table | None,
    repo_snapshot: pa.Table | None,
    scip_changed_file_ids: Sequence[str],
) -> pa.Table:
    """Build the base impact table from changed/deleted files.

    Returns
    -------
    pa.Table
        Impacted file table seeded from repo/SCIP changes.
    """
    schema = dataset_schema("inc_impacted_files_v2")
    if not file_changes.changed_file_ids and not file_changes.deleted_file_ids:
        return empty_table(schema)

    path_lookup = _path_lookup(incremental_diff, repo_snapshot)
    file_ids: list[str] = []
    paths: list[str] = []
    reason_kinds: list[str] = []
    reason_refs: list[str] = []

    for file_id in file_changes.changed_file_ids:
        path = _resolve_path(path_lookup, file_id)
        file_ids.append(file_id)
        paths.append(path)
        reason_kinds.append("changed")
        reason_refs.append("repo_diff")

    for file_id in file_changes.deleted_file_ids:
        path = _resolve_path(path_lookup, file_id)
        file_ids.append(file_id)
        paths.append(path)
        reason_kinds.append("deleted")
        reason_refs.append("repo_diff")

    for file_id in scip_changed_file_ids:
        if file_id in file_changes.changed_file_ids or file_id in file_changes.deleted_file_ids:
            continue
        path = _resolve_path(path_lookup, file_id)
        file_ids.append(file_id)
        paths.append(path)
        reason_kinds.append("scip_changed")
        reason_refs.append("scip_diff")

    if not file_ids:
        return empty_table(schema)
    return table_from_arrays(
        schema,
        columns={
            "file_id": pa.array(file_ids, type=pa.string()),
            "path": pa.array(paths, type=pa.string()),
            "reason_kind": pa.array(reason_kinds, type=pa.string()),
            "reason_ref": pa.array(reason_refs, type=pa.string()),
        },
        num_rows=len(file_ids),
    )


def _export_import_keys(changed_exports: pa.Table) -> pa.Table:
    seen: set[tuple[str, str | None, str]] = set()
    module_fqns: list[str] = []
    names: list[str | None] = []
    qnames: list[str] = []
    for row in iter_table_rows(changed_exports):
        qname = row.get("qname")
        if not isinstance(qname, str) or not qname:
            continue
        module_fqn, name = _split_qname(qname)
        if module_fqn is None:
            continue
        key = (module_fqn, name, qname)
        if key in seen:
            continue
        seen.add(key)
        module_fqns.append(module_fqn)
        names.append(name)
        qnames.append(qname)

    if not module_fqns:
        return table_from_arrays(_EXPORT_KEY_SCHEMA, columns={}, num_rows=0)
    return table_from_arrays(
        _EXPORT_KEY_SCHEMA,
        columns={
            "module_fqn": pa.array(module_fqns, type=pa.string()),
            "name": pa.array(names, type=pa.string()),
            "qname": pa.array(qnames, type=pa.string()),
        },
        num_rows=len(module_fqns),
    )


def _split_qname(qname: str) -> tuple[str | None, str | None]:
    parts = [part for part in qname.split(".") if part]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return ".".join(parts[:-1]), parts[-1]


def _path_lookup(
    incremental_diff: pa.Table | None,
    repo_snapshot: pa.Table | None,
) -> dict[str, str]:
    lookup: dict[str, str] = {}
    if incremental_diff is not None:
        for row in iter_table_rows(incremental_diff):
            file_id = row.get("file_id")
            path = row.get("path")
            if isinstance(file_id, str) and isinstance(path, str):
                lookup[file_id] = path
    if repo_snapshot is not None:
        for row in iter_table_rows(repo_snapshot):
            file_id = row.get("file_id")
            path = row.get("path")
            if isinstance(file_id, str) and isinstance(path, str):
                lookup.setdefault(file_id, path)
    return lookup


def _resolve_path(path_lookup: Mapping[str, str], file_id: str) -> str:
    path = path_lookup.get(file_id)
    if path is None:
        msg = f"Missing path for impacted file_id={file_id!r}"
        raise ValueError(msg)
    return path


def _optional_tables(
    tables: Sequence[TableLike | None],
    *,
    schema: pa.Schema,
) -> list[pa.Table]:
    out: list[pa.Table] = []
    for table in tables:
        if table is None:
            continue
        casted = cast("pa.Table", table)
        if casted.num_rows == 0:
            continue
        out.append(align_table(casted, schema=schema, safe_cast=True))
    return out


def _union_all_tables(tables: Sequence[ibis.Table]) -> ibis.Table:
    if not tables:
        msg = "Expected at least one table to union."
        raise ValueError(msg)
    combined: ibis.Table = tables[0]
    for table in tables[1:]:
        combined = ibis.union(combined, table, distinct=False)
    return combined


__all__ = [
    "changed_file_impact_table",
    "impacted_callers_from_changed_exports",
    "impacted_importers_from_changed_exports",
    "import_closure_only_from_changed_exports",
    "merge_impacted_files",
]
