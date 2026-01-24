"""
DataFusion catalog introspection snapshot.

This module provides point-in-time snapshots of DataFusion catalog state
including tables, columns, functions, and settings. Snapshots ensure
consistency within a compilation unit and enable schema-driven optimization.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass
class IntrospectionSnapshot:
    """
    Point-in-time snapshot of DataFusion catalog state.

    All schema/function queries should go through this snapshot
    to ensure consistency within a compilation unit.

    Attributes
    ----------
    tables : pa.Table
        Table metadata from information_schema.tables
    columns : pa.Table
        Column metadata from information_schema.columns
    routines : pa.Table | None
        Function metadata from information_schema.routines (if available)
    parameters : pa.Table | None
        Function parameter metadata from information_schema.parameters (if available)
    settings : pa.Table
        DataFusion configuration from information_schema.df_settings
    """

    tables: pa.Table
    columns: pa.Table
    routines: pa.Table | None
    parameters: pa.Table | None
    settings: pa.Table

    @classmethod
    def capture(cls, ctx: SessionContext) -> IntrospectionSnapshot:
        """
        Capture snapshot from SessionContext.

        Queries information_schema views to extract catalog metadata.
        Gracefully handles missing routines/parameters for backends that
        don't support function introspection.

        Parameters
        ----------
        ctx : SessionContext
            DataFusion session to introspect

        Returns
        -------
        IntrospectionSnapshot
            Snapshot containing all available catalog metadata
        """
        tables = ctx.sql("""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM information_schema.tables
        """).to_arrow_table()

        columns = ctx.sql("""
            SELECT
                table_catalog, table_schema, table_name,
                column_name, ordinal_position, data_type,
                is_nullable, column_default
            FROM information_schema.columns
            ORDER BY table_catalog, table_schema, table_name, ordinal_position
        """).to_arrow_table()

        settings = ctx.sql("""
            SELECT name, value
            FROM information_schema.df_settings
        """).to_arrow_table()

        # Routines may not be available in all configurations
        try:
            routines = ctx.sql("""
                SELECT
                    specific_catalog, specific_schema, specific_name,
                    routine_catalog, routine_schema, routine_name,
                    routine_type, data_type
                FROM information_schema.routines
            """).to_arrow_table()

            parameters = ctx.sql("""
                SELECT
                    specific_catalog, specific_schema, specific_name,
                    ordinal_position, parameter_mode, parameter_name,
                    data_type
                FROM information_schema.parameters
                ORDER BY specific_name, ordinal_position
            """).to_arrow_table()
        except Exception:  # noqa: BLE001
            # DataFusion versions may not expose routines/parameters views
            routines = None
            parameters = None

        return cls(
            tables=tables,
            columns=columns,
            routines=routines,
            parameters=parameters,
            settings=settings,
        )

    def schema_map(self) -> dict[str, dict[str, str]]:
        """
        Build schema mapping for SQLGlot optimizer.

        Returns
        -------
        dict[str, dict[str, str]]
            Mapping of table_name -> {column_name: data_type}
        """
        result: dict[str, dict[str, str]] = {}

        for row in self.columns.to_pylist():
            table_name = row["table_name"]
            if table_name not in result:
                result[table_name] = {}
            result[table_name][row["column_name"]] = row["data_type"]

        return result

    def qualified_schema_map(self) -> dict[str, dict[str, dict[str, str]]]:
        """
        Build fully qualified schema mapping.

        Returns
        -------
        dict[str, dict[str, dict[str, str]]]
            Mapping of catalog.schema -> table -> {column: type}
        """
        result: dict[str, dict[str, dict[str, str]]] = {}

        for row in self.columns.to_pylist():
            db_key = f"{row['table_catalog']}.{row['table_schema']}"
            table_name = row["table_name"]

            if db_key not in result:
                result[db_key] = {}
            if table_name not in result[db_key]:
                result[db_key][table_name] = {}

            result[db_key][table_name][row["column_name"]] = row["data_type"]

        return result

    def table_exists(self, name: str) -> bool:
        """
        Check if table exists in snapshot.

        Parameters
        ----------
        name : str
            Table name to check

        Returns
        -------
        bool
            True if table exists in catalog
        """
        import pyarrow.compute as pc

        return pc.any(pc.equal(self.tables["table_name"], name)).as_py()  # type: ignore[attr-defined]

    def get_table_columns(self, name: str) -> list[tuple[str, str]]:
        """
        Get columns for a table as (name, type) pairs.

        Parameters
        ----------
        name : str
            Table name to query

        Returns
        -------
        list[tuple[str, str]]
            List of (column_name, data_type) tuples ordered by position
        """
        import pyarrow.compute as pc

        mask = pc.equal(self.columns["table_name"], name)  # type: ignore[attr-defined]
        filtered = self.columns.filter(mask)

        return [(row["column_name"], row["data_type"]) for row in filtered.to_pylist()]

    def function_signatures(self) -> dict[str, list[tuple[list[str], str]]]:
        """
        Build function signature map.

        Extracts function signatures from routines and parameters metadata.
        Returns empty dict if function introspection is unavailable.

        Returns
        -------
        dict[str, list[tuple[list[str], str]]]
            Mapping of function_name -> [(param_types, return_type), ...]
        """
        if self.routines is None or self.parameters is None:
            return {}

        result: dict[str, list[tuple[list[str], str]]] = {}

        # Group parameters by function
        param_map: dict[str, list[str]] = {}
        for row in self.parameters.to_pylist():
            fn_key = row["specific_name"]
            if fn_key not in param_map:
                param_map[fn_key] = []
            param_map[fn_key].append(row["data_type"])

        # Build signatures
        for row in self.routines.to_pylist():
            fn_name = row["routine_name"]
            specific_name = row["specific_name"]
            return_type = row["data_type"]

            param_types = param_map.get(specific_name, [])

            if fn_name not in result:
                result[fn_name] = []

            result[fn_name].append((param_types, return_type))

        return result


class IntrospectionCache:
    """
    Cached introspection snapshot with invalidation support.

    Holds a single snapshot and provides invalidation hooks for DDL operations.
    Automatically recaptures when accessed after invalidation.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session to introspect

    Attributes
    ----------
    snapshot : IntrospectionSnapshot
        Current or recaptured snapshot (property)
    """

    def __init__(self, ctx: SessionContext) -> None:
        """Initialize cache with SessionContext."""
        self._ctx = ctx
        self._snapshot: IntrospectionSnapshot | None = None
        self._invalidated = False

    @property
    def snapshot(self) -> IntrospectionSnapshot:
        """
        Get or capture snapshot.

        Automatically recaptures if cache is invalidated or empty.

        Returns
        -------
        IntrospectionSnapshot
            Current snapshot
        """
        if self._snapshot is None or self._invalidated:
            self._snapshot = IntrospectionSnapshot.capture(self._ctx)
            self._invalidated = False
        return self._snapshot

    def invalidate(self) -> None:
        """
        Mark cache as stale (call after DDL changes).

        Next access to `snapshot` property will trigger recapture.
        """
        self._invalidated = True

    def schema_map(self) -> dict[str, dict[str, str]]:
        """
        Get schema map from current snapshot.

        Returns
        -------
        dict[str, dict[str, str]]
            Schema map from current snapshot
        """
        return self.snapshot.schema_map()
