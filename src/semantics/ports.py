"""Port protocols for engine-agnostic semantic compilation."""

from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class DataFramePort(Protocol):
    """Abstract dataframe operations used by semantic compilation."""

    def select(self, *columns: str) -> DataFramePort:
        """Project columns from the dataframe."""
        ...

    def filter(self, predicate: object) -> DataFramePort:
        """Filter dataframe rows by predicate."""
        ...

    def join(self, right: DataFramePort, on: str) -> DataFramePort:
        """Join two dataframes by key expression."""
        ...

    def collect(self) -> pa.Table:
        """Collect dataframe rows into an Arrow table."""
        ...


class SessionPort(Protocol):
    """Abstract session operations consumed by semantic pipelines."""

    def sql(self, query: str) -> DataFramePort:
        """Execute SQL and return a dataframe-like result."""
        ...

    def register_table(self, name: str, table: pa.Table) -> None:
        """Register an Arrow table under a dataset name."""
        ...

    def table(self, name: str) -> DataFramePort:
        """Return a dataframe-like handle for a registered table."""
        ...

    def table_names(self) -> list[str]:
        """List available table names."""
        ...


class UdfResolverPort(Protocol):
    """Abstract UDF resolution surface used by semantic compiler paths."""

    def resolve_udf(self, name: str) -> object:
        """Resolve a UDF by name."""
        ...

    def udf_expr(self, name: str, *args: object) -> object:
        """Build a UDF expression with runtime arguments."""
        ...


class SchemaProviderPort(Protocol):
    """Abstract schema-provider surface."""

    def table_schema(self, table_name: str) -> pa.Schema:
        """Return schema for a registered table."""
        ...


class OutputWriterPort(Protocol):
    """Abstract output-writing surface."""

    def write_table(self, *, name: str, table: pa.Table) -> None:
        """Persist a named Arrow table."""
        ...
