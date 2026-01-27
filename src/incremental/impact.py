"""Incremental impact closure helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast
from uuid import uuid4

import pyarrow as pa
from datafusion import col, lit
from datafusion.dataframe import DataFrame

from arrow_utils.core.array_iter import iter_table_rows
from arrow_utils.core.interop import TableLike
from arrow_utils.schema.build import empty_table, table_from_arrays
from datafusion_engine.schema_alignment import align_table
from incremental.delta_context import DeltaAccessContext, register_delta_df
from incremental.plan_bundle_exec import execute_df_to_table
from incremental.registry_specs import dataset_schema
from incremental.runtime import IncrementalRuntime, TempTableRegistry
from incremental.types import IncrementalFileChanges

_EXPORT_KEY_SCHEMA = pa.schema(
    [
        pa.field("module_fqn", pa.string()),
        pa.field("name", pa.string()),
        pa.field("qname", pa.string()),
    ]
)


def _df_from_arrow(
    runtime: IncrementalRuntime,
    table: pa.Table,
    *,
    registry: TempTableRegistry,
    prefix: str,
) -> DataFrame:
    name = registry.register_table(table, prefix=prefix)
    return runtime.session_runtime().ctx.table(name)


def _df_from_delta(
    runtime: IncrementalRuntime,
    path: str,
    *,
    registry: TempTableRegistry,
    prefix: str,
) -> DataFrame:
    name = f"__incremental_{prefix}_{uuid4().hex}"
    df = register_delta_df(
        DeltaAccessContext(runtime=runtime),
        path=path,
        name=name,
    )
    registry.track(name)
    return df


def _union_all_frames(frames: Sequence[DataFrame]) -> DataFrame:
    if not frames:
        msg = "At least one DataFrame is required to build a union."
        raise ValueError(msg)
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.union(frame)
    return combined


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
    schema = dataset_schema("inc_impacted_callers_v1")
    changed = cast("pa.Table", changed_exports)
    if changed.num_rows == 0:
        return empty_table(schema)

    pieces: list[DataFrame] = []
    with TempTableRegistry(runtime) as registry:
        changed_table = _df_from_arrow(
            runtime,
            changed,
            registry=registry,
            prefix="changed_exports",
        )
        if prev_rel_callsite_qname is not None:
            rel_qname = _df_from_delta(
                runtime,
                prev_rel_callsite_qname,
                registry=registry,
                prefix="rel_callsite_qname",
            )
            changed_qname = changed_table.filter(col("qname_id").is_not_null())
            qname_hits = rel_qname.join(changed_qname, "qname_id", how="inner")
            qname_hits = qname_hits.select(
                col("edge_owner_file_id").alias("file_id"),
                col("path"),
                lit("callsite_qname").alias("reason_kind"),
                col("qname_id").alias("reason_ref"),
            )
            pieces.append(qname_hits)

        if prev_rel_callsite_symbol is not None:
            rel_symbol = _df_from_delta(
                runtime,
                prev_rel_callsite_symbol,
                registry=registry,
                prefix="rel_callsite_symbol",
            )
            changed_symbol = changed_table.filter(col("symbol").is_not_null())
            symbol_hits = rel_symbol.join(changed_symbol, "symbol", how="inner")
            symbol_hits = symbol_hits.select(
                col("edge_owner_file_id").alias("file_id"),
                col("path"),
                lit("callsite_symbol").alias("reason_kind"),
                col("symbol").alias("reason_ref"),
            )
            pieces.append(symbol_hits)

        if not pieces:
            return empty_table(schema)
        combined = _union_all_frames(pieces).distinct()
        result = execute_df_to_table(runtime, combined, view_name="incremental_impacted_callers")
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
    schema = dataset_schema("inc_impacted_importers_v1")
    if prev_imports_resolved is None:
        return empty_table(schema)
    changed = cast("pa.Table", changed_exports)
    if changed.num_rows == 0:
        return empty_table(schema)
    exports = _export_import_keys(changed)
    if exports.num_rows == 0:
        return empty_table(schema)

    pieces: list[DataFrame] = []
    with TempTableRegistry(runtime) as registry:
        imports_resolved = _df_from_delta(
            runtime,
            prev_imports_resolved,
            registry=registry,
            prefix="imports_resolved",
        )
        exports_table = _df_from_arrow(
            runtime,
            exports,
            registry=registry,
            prefix="export_keys",
        )
        by_name = imports_resolved.join(
            exports_table,
            ["imported_module_fqn", "imported_name"],
            how="inner",
        )
        by_name = by_name.select(
            col("importer_file_id").alias("file_id"),
            col("importer_path").alias("path"),
            lit("import_name").alias("reason_kind"),
            col("qname").alias("reason_ref"),
        )
        pieces.append(by_name)

        star = imports_resolved.filter(col("is_star") == lit(value=True))
        by_star = star.join(
            exports_table,
            "imported_module_fqn",
            how="inner",
        )
        by_star = by_star.select(
            col("importer_file_id").alias("file_id"),
            col("importer_path").alias("path"),
            lit("import_star").alias("reason_kind"),
            col("qname").alias("reason_ref"),
        )
        pieces.append(by_star)

        combined = _union_all_frames(pieces).distinct()
        result = execute_df_to_table(runtime, combined, view_name="incremental_impacted_importers")
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
    schema = dataset_schema("inc_impacted_importers_v1")
    if prev_imports_resolved is None:
        return empty_table(schema)
    changed = cast("pa.Table", changed_exports)
    if changed.num_rows == 0:
        return empty_table(schema)
    exports = _export_import_keys(changed)
    if exports.num_rows == 0:
        return empty_table(schema)

    pieces: list[DataFrame] = []
    with TempTableRegistry(runtime) as registry:
        imports_resolved = _df_from_delta(
            runtime,
            prev_imports_resolved,
            registry=registry,
            prefix="imports_resolved",
        )
        exports_table = _df_from_arrow(
            runtime,
            exports,
            registry=registry,
            prefix="export_keys",
        )
        module_imports = imports_resolved.filter(
            col("imported_name").is_null() & (col("is_star") == lit(value=False))
        )
        module_hits = module_imports.join(
            exports_table,
            "imported_module_fqn",
            how="inner",
        )
        module_hits = module_hits.select(
            col("importer_file_id").alias("file_id"),
            col("importer_path").alias("path"),
            lit("import_module").alias("reason_kind"),
            col("qname").alias("reason_ref"),
        )
        pieces.append(module_hits)

        star = imports_resolved.filter(col("is_star") == lit(value=True))
        star_hits = star.join(
            exports_table,
            "imported_module_fqn",
            how="inner",
        )
        star_hits = star_hits.select(
            col("importer_file_id").alias("file_id"),
            col("importer_path").alias("path"),
            lit("import_star").alias("reason_kind"),
            col("qname").alias("reason_ref"),
        )
        pieces.append(star_hits)

        combined = _union_all_frames(pieces).distinct()
        result = execute_df_to_table(runtime, combined, view_name="incremental_impacted_exports")
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

    frames: list[DataFrame] = []
    with TempTableRegistry(runtime) as registry:
        for idx, table in enumerate(tables):
            if table.num_rows == 0:
                continue
            frames.append(
                _df_from_arrow(
                    runtime,
                    table,
                    registry=registry,
                    prefix=f"impacted_{idx}",
                )
            )
        if not frames:
            return empty_table(schema)
        combined = _union_all_frames(frames).distinct()
        result = execute_df_to_table(runtime, combined, view_name="incremental_impacted_defs")
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


__all__ = [
    "changed_file_impact_table",
    "impacted_callers_from_changed_exports",
    "impacted_importers_from_changed_exports",
    "import_closure_only_from_changed_exports",
    "merge_impacted_files",
]
