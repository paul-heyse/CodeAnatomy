"""DataFusion-backed schema context for relspec validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.schema_introspection import (
    SchemaIntrospector,
    schema_map_for_sqlglot,
)
from datafusion_engine.schema_registry import is_nested_dataset, nested_schema_for
from datafusion_engine.table_provider_metadata import (
    TableProviderMetadata,
    all_table_provider_metadata,
    table_provider_metadata,
)
from relspec.errors import RelspecValidationError
from sqlglot_tools.optimizer import SchemaMapping

if TYPE_CHECKING:
    from engine.session import EngineSession


@dataclass(frozen=True)
class RelspecSchemaContext:
    """Resolve dataset schema metadata from a DataFusion session.

    Parameters
    ----------
    ctx
        DataFusion session context.
    introspector
        Schema introspection helper bound to the session.
    """

    ctx: SessionContext
    introspector: SchemaIntrospector

    @classmethod
    def from_session(cls, ctx: SessionContext) -> RelspecSchemaContext:
        """Build a schema context from a DataFusion session.

        Parameters
        ----------
        ctx
            DataFusion session context.

        Returns
        -------
        RelspecSchemaContext
            Schema context bound to the provided session.
        """
        from datafusion_engine.runtime import sql_options_for_profile

        return cls(
            ctx=ctx,
            introspector=SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None)),
        )

    @classmethod
    def from_engine_session(cls, engine_session: EngineSession) -> RelspecSchemaContext:
        """Build a schema context from an engine session.

        Parameters
        ----------
        engine_session
            Engine session with a DataFusion context.

        Returns
        -------
        RelspecSchemaContext
            Schema context bound to the engine session.

        Raises
        ------
        RelspecValidationError
            Raised when the engine session lacks a DataFusion context.
        """
        session_ctx = engine_session.df_ctx()
        if session_ctx is None:
            msg = "EngineSession does not have a DataFusion SessionContext."
            raise RelspecValidationError(msg)
        return cls.from_session(session_ctx)

    def dataset_names(self) -> tuple[str, ...]:
        """Return dataset names from information_schema.

        Returns
        -------
        tuple[str, ...]
            Dataset names sorted by name.
        """
        names: set[str] = set()
        for row in self.introspector.tables_snapshot():
            table_name = row.get("table_name")
            table_schema = row.get("table_schema")
            if table_schema == "information_schema":
                continue
            if isinstance(table_name, str):
                names.add(table_name)
        return tuple(sorted(names))

    def dataset_schema(self, name: str) -> pa.Schema | None:
        """Return the Arrow schema for a dataset when available.

        Parameters
        ----------
        name
            Dataset name to resolve.

        Returns
        -------
        pyarrow.Schema | None
            Arrow schema when resolvable, otherwise ``None``.
        """
        if is_nested_dataset(name):
            return _coerce_schema(nested_schema_for(name, allow_derived=True))
        try:
            return _coerce_schema(self.ctx.table(name).schema())
        except (KeyError, RuntimeError, TypeError, ValueError):
            return None

    def describe_query(self, sql: str) -> list[dict[str, object]]:
        """Return computed schema rows for a SQL query.

        Parameters
        ----------
        sql
            SQL query to describe.

        Returns
        -------
        list[dict[str, object]]
            Rows from ``DESCRIBE`` for the query.
        """
        return self.introspector.describe_query(sql)

    def table_columns(self, table_name: str) -> list[dict[str, object]]:
        """Return column metadata rows for a dataset.

        Parameters
        ----------
        table_name
            Dataset name to inspect.

        Returns
        -------
        list[dict[str, object]]
            Column metadata rows.
        """
        return self.introspector.table_columns(table_name)

    def schema_map(self) -> SchemaMapping:
        """Return a SQLGlot-compatible schema mapping.

        Returns
        -------
        SchemaMapping
            Nested schema mapping suitable for SQLGlot qualification.
        """
        return schema_map_for_sqlglot(self.introspector)

    def schema_map_fingerprint(self) -> str:
        """Return a stable fingerprint for the schema mapping.

        Returns
        -------
        str
            Hash fingerprint for the schema map.
        """
        return self.introspector.schema_map_fingerprint()

    def table_provider_metadata(self, table_name: str) -> TableProviderMetadata | None:
        """Return TableProvider metadata for a dataset when available.

        Returns
        -------
        TableProviderMetadata | None
            TableProvider metadata for the dataset, if available.
        """
        return table_provider_metadata(id(self.ctx), table_name=table_name)

    def all_table_provider_metadata(self) -> dict[str, TableProviderMetadata]:
        """Return all TableProvider metadata for the session context.

        Returns
        -------
        dict[str, TableProviderMetadata]
            Mapping of table names to provider metadata.
        """
        return all_table_provider_metadata(id(self.ctx))


def _coerce_schema(schema: object | None) -> pa.Schema | None:
    """Coerce a schema-like object into a pyarrow.Schema.

    Parameters
    ----------
    schema
        Schema-like object to resolve.

    Returns
    -------
    pyarrow.Schema | None
        Resolved Arrow schema, or ``None`` when no schema is provided.

    Raises
    ------
    TypeError
        Raised when the schema cannot be resolved to a pyarrow.Schema.
    """
    if schema is None:
        return None
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Schema did not resolve to pyarrow.Schema."
    raise TypeError(msg)


__all__ = ["RelspecSchemaContext"]
