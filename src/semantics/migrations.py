"""Versioned schema migration registry for semantic datasets."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from semantics.schema_diff import SchemaDiff

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


MigrationKey = tuple[str, str]
MigrationFn = Callable[["SessionContext"], "DataFrame"]


@dataclass(frozen=True)
class MigrationSpec:
    """Migration specification for a dataset version bump."""

    source: str
    target: str
    migrate: MigrationFn


MIGRATIONS: Final[dict[MigrationKey, MigrationSpec]] = {}


def _ensure_span_column(df: DataFrame) -> DataFrame:
    """Ensure a span column is present on a DataFrame.

    Parameters
    ----------
    df
        DataFrame to inspect.

    Returns
    -------
    DataFrame
        DataFrame with span column ensured.
    """
    if "span" in df.schema().names:
        return df
    from datafusion import col

    from datafusion_engine.udf.shims import span_make

    return df.with_column("span", span_make(col("bstart"), col("bend")))


def migrate_cpg_nodes_v1_to_v2(ctx: SessionContext) -> DataFrame:
    """Migrate cpg_nodes_v1 to cpg_nodes_v2 by ensuring span presence.

    Returns
    -------
    DataFrame
        Migrated DataFrame with span column.
    """
    df = ctx.table("cpg_nodes_v1")
    return _ensure_span_column(df)


def migrate_cpg_edges_v1_to_v2(ctx: SessionContext) -> DataFrame:
    """Migrate cpg_edges_v1 to cpg_edges_v2 by ensuring span presence.

    Returns
    -------
    DataFrame
        Migrated DataFrame with span column.
    """
    df = ctx.table("cpg_edges_v1")
    return _ensure_span_column(df)


def register_migration(source: str, target: str, migrate: MigrationFn) -> None:
    """Register a migration from source to target dataset name."""
    key = (source, target)
    MIGRATIONS[key] = MigrationSpec(source=source, target=target, migrate=migrate)


def migration_for(source: str, target: str) -> MigrationFn | None:
    """Return a migration function for the given source/target pair.

    Returns
    -------
    MigrationFn | None
        Migration function when registered, otherwise ``None``.
    """
    spec = MIGRATIONS.get((source, target))
    if spec is None:
        return None
    return spec.migrate


def migration_skeleton(source: str, target: str, diff: SchemaDiff) -> str:
    """Return a migration skeleton snippet for a schema diff."""
    function_name = _migration_fn_name(source, target)
    diff_lines = diff.summary_lines()
    change_lines = diff_lines or ("no schema changes detected",)
    change_comment = "\n".join(f"    # - {line}" for line in change_lines)
    return (
        f"def {function_name}(ctx: SessionContext) -> DataFrame:\n"
        f"    \"\"\"Migrate {source} to {target}.\"\"\"\n"
        f"    df = ctx.table(\"{source}\")\n"
        f"{change_comment}\n"
        f"    return df\n\n"
        f"register_migration(\"{source}\", \"{target}\", {function_name})\n"
    )


def _migration_fn_name(source: str, target: str) -> str:
    normalized = f"migrate_{source}_to_{target}"
    sanitized = re.sub(r"[^0-9a-zA-Z_]+", "_", normalized)
    return sanitized.strip("_").lower()


register_migration("cpg_nodes_v1", "cpg_nodes_v2", migrate_cpg_nodes_v1_to_v2)
register_migration("cpg_edges_v1", "cpg_edges_v2", migrate_cpg_edges_v1_to_v2)

__all__ = [
    "MIGRATIONS",
    "MigrationFn",
    "MigrationKey",
    "MigrationSpec",
    "migrate_cpg_edges_v1_to_v2",
    "migrate_cpg_nodes_v1_to_v2",
    "migration_for",
    "migration_skeleton",
    "register_migration",
]
