"""Module index builder for incremental imports resolution."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import cast

import pyarrow as pa
from libcst import helpers as cst_helpers

from arrowdsl.core.ids import iter_array_values
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_arrays
from incremental.registry_specs import dataset_schema


def _fallback_module_name(path: str) -> tuple[str | None, bool]:
    posix_path = PurePosixPath(path)
    if posix_path.suffix != ".py":
        return None, False
    is_init = posix_path.name == "__init__.py"
    stem_parts = posix_path.with_suffix("").parts
    if is_init:
        stem_parts = stem_parts[:-1]
    if not stem_parts:
        return None, is_init
    return ".".join(stem_parts), is_init


def _module_name_from_paths(
    *,
    repo_root: Path,
    rel_path: str | None,
    abs_path: str | None,
) -> tuple[str | None, bool]:
    if rel_path is None:
        return None, False
    abs_value = Path(abs_path) if abs_path else repo_root / rel_path
    try:
        result = cst_helpers.calculate_module_and_package(str(repo_root), str(abs_value))
    except (OSError, ValueError):
        return _fallback_module_name(rel_path)
    module_name = result.name
    if module_name is None:
        return _fallback_module_name(rel_path)
    return module_name, rel_path.endswith(("/__init__.py", "__init__.py"))


def build_module_index(
    repo_files: TableLike,
    *,
    repo_root: Path,
) -> pa.Table:
    """Build a per-file module index table.

    Returns
    -------
    pa.Table
        Module index table with module_fqn and is_init columns.
    """
    table = cast("pa.Table", repo_files)
    if table.num_rows == 0:
        return table_from_arrays(dataset_schema("py_module_index_v1"), columns={}, num_rows=0)
    rel_paths = table["path"] if "path" in table.column_names else pa.nulls(table.num_rows)
    abs_paths = table["abs_path"] if "abs_path" in table.column_names else pa.nulls(table.num_rows)
    module_names: list[str | None] = []
    is_init_flags: list[bool] = []
    for rel_path, abs_path in zip(
        iter_array_values(rel_paths),
        iter_array_values(abs_paths),
        strict=True,
    ):
        module_name, is_init = _module_name_from_paths(
            repo_root=repo_root,
            rel_path=cast("str | None", rel_path),
            abs_path=cast("str | None", abs_path),
        )
        module_names.append(module_name)
        is_init_flags.append(is_init)
    schema = dataset_schema("py_module_index_v1")
    return table_from_arrays(
        schema,
        columns={
            "file_id": table["file_id"],
            "path": rel_paths,
            "module_fqn": pa.array(module_names, type=pa.string()),
            "is_init": pa.array(is_init_flags, type=pa.bool_()),
        },
        num_rows=table.num_rows,
    )


__all__ = ["build_module_index"]
