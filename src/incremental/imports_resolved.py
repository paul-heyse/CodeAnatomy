"""Resolved imports builder for incremental impact analysis."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pyarrow as pa

from arrow_utils.core.array_iter import iter_table_rows
from datafusion_engine.arrow.build import empty_table, table_from_columns
from datafusion_engine.arrow.interop import TableLike
from incremental.registry_specs import dataset_schema


def _importer_base_module(module_fqn: str | None, *, is_init: bool) -> str | None:
    if module_fqn is None:
        return None
    if is_init:
        return module_fqn
    if "." not in module_fqn:
        return ""
    return module_fqn.rsplit(".", 1)[0]


def _apply_relative(base: str | None, *, level: int) -> str | None:
    if base is None:
        return None
    parts = [part for part in base.split(".") if part]
    if level <= 0:
        return base or None
    if level > len(parts) + 1:
        return None
    keep = len(parts) - (level - 1)
    if keep <= 0:
        return None
    return ".".join(parts[:keep])


def _join_module(base: str | None, module: str | None) -> str | None:
    if module and base:
        return f"{base}.{module}"
    if module:
        return module
    return base


def _empty_resolved_imports() -> pa.Table:
    return empty_table(dataset_schema("py_imports_resolved_v1"))


def _module_index_map(
    module_index: pa.Table,
) -> dict[str, tuple[str | None, bool]]:
    module_map: dict[str, tuple[str | None, bool]] = {}
    for row in iter_table_rows(module_index):
        file_id = row.get("file_id")
        if not isinstance(file_id, str):
            continue
        module_fqn = row.get("module_fqn")
        is_init = bool(row.get("is_init") or False)
        module_map[file_id] = (cast("str | None", module_fqn), is_init)
    return module_map


def _parse_import_row(
    row: dict[str, object],
) -> tuple[str, str, str | None, str | None, str, int, bool] | None:
    importer_file_id = row.get("file_id")
    importer_path = row.get("path")
    if not isinstance(importer_file_id, str) or not isinstance(importer_path, str):
        return None
    kind = row.get("kind")
    module = row.get("module")
    name = row.get("name")
    relative_level = row.get("relative_level")
    is_star = bool(row.get("is_star") or False)
    kind_value = str(kind).lower() if kind is not None else ""
    module_value = cast("str | None", module) if isinstance(module, str) else None
    name_value = cast("str | None", name) if isinstance(name, str) else None
    level_value = int(relative_level) if isinstance(relative_level, int) else 0
    return (
        importer_file_id,
        importer_path,
        module_value,
        name_value,
        kind_value,
        level_value,
        is_star,
    )


def _resolve_import_row(
    row: dict[str, object],
    *,
    module_map: Mapping[str, tuple[str | None, bool]],
) -> tuple[str, str, str, str | None, bool] | None:
    coerced = _parse_import_row(row)
    if coerced is None:
        return None
    (
        importer_file_id,
        importer_path,
        module_value,
        name_value,
        kind_value,
        level_value,
        is_star,
    ) = coerced
    resolved_module: str | None
    resolved_name: str | None
    if kind_value == "import":
        resolved_module = name_value
        resolved_name = None
    elif kind_value == "importfrom":
        module_fqn, is_init = module_map.get(importer_file_id, (None, False))
        base = _importer_base_module(module_fqn, is_init=is_init)
        base = _apply_relative(base, level=level_value)
        resolved_module = _join_module(base, module_value)
        resolved_name = None if is_star or name_value == "*" else name_value
    else:
        return None
    if resolved_module is None:
        return None
    return (
        importer_file_id,
        importer_path,
        resolved_module,
        resolved_name,
        is_star,
    )


def resolve_imports(
    cst_imports_norm: TableLike,
    module_index: TableLike,
) -> pa.Table:
    """Resolve normalized imports to absolute module names.

    Returns
    -------
    pa.Table
        Resolved imports table with module/name mappings.
    """
    imports_table = cast("pa.Table", cst_imports_norm)
    if imports_table.num_rows == 0:
        return _empty_resolved_imports()
    index_table = cast("pa.Table", module_index)
    module_map = _module_index_map(index_table)

    importer_file_ids: list[str] = []
    importer_paths: list[str] = []
    imported_modules: list[str] = []
    imported_names: list[str | None] = []
    is_star_flags: list[bool] = []
    for row in iter_table_rows(imports_table):
        resolved = _resolve_import_row(row, module_map=module_map)
        if resolved is None:
            continue
        importer_file_id, importer_path, resolved_module, resolved_name, is_star = resolved
        importer_file_ids.append(importer_file_id)
        importer_paths.append(importer_path)
        imported_modules.append(resolved_module)
        imported_names.append(resolved_name)
        is_star_flags.append(is_star)

    if not importer_file_ids:
        return _empty_resolved_imports()
    schema = dataset_schema("py_imports_resolved_v1")
    return table_from_columns(
        schema,
        {
            "importer_file_id": pa.array(importer_file_ids, type=pa.string()),
            "importer_path": pa.array(importer_paths, type=pa.string()),
            "imported_module_fqn": pa.array(imported_modules, type=pa.string()),
            "imported_name": pa.array(imported_names, type=pa.string()),
            "is_star": pa.array(is_star_flags, type=pa.bool_()),
        },
    )


__all__ = ["resolve_imports"]
