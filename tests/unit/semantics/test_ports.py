"""Unit tests for semantics port protocols."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from semantics.ports import (
    OutputWriterPort,
    SchemaProviderPort,
    SessionContextProviderPort,
    SessionPort,
)


class _DataFrameImpl:
    def select(self, *columns: str) -> _DataFrameImpl:
        _ = columns
        return self

    def filter(self, predicate: object) -> _DataFrameImpl:
        _ = predicate
        return self

    def join(self, right: _DataFrameImpl, on: str) -> _DataFrameImpl:
        _ = (right, on)
        return self

    @staticmethod
    def collect() -> pa.Table:
        return pa.table({"id": [1]})


class _SessionImpl:
    @staticmethod
    def sql(query: str) -> _DataFrameImpl:
        _ = query
        return _DataFrameImpl()

    @staticmethod
    def register_table(name: str, table: pa.Table) -> None:
        _ = (name, table)

    @staticmethod
    def table_names() -> list[str]:
        return ["t1"]


class _SchemaProviderImpl:
    @staticmethod
    def table_schema(table_name: str) -> pa.Schema:
        _ = table_name
        return pa.schema([("id", pa.int64())])


class _OutputWriterImpl:
    @staticmethod
    def write_table(*, name: str, table: pa.Table) -> None:
        _ = (name, table)


class _SessionContextProviderImpl:
    @staticmethod
    def session_context() -> object:
        return object()


def test_session_port_protocol() -> None:
    """Session and dataframe protocol implementations interoperate."""
    session = cast("SessionPort", _SessionImpl())
    df = session.sql("select 1")
    assert df.collect().num_rows == 1


def test_schema_and_output_ports() -> None:
    """Schema provider and output writer protocol implementations interoperate."""
    schema_provider = cast("SchemaProviderPort", _SchemaProviderImpl())
    writer = cast("OutputWriterPort", _OutputWriterImpl())
    schema = schema_provider.table_schema("t1")
    writer.write_table(name="t1", table=pa.table({"id": [1]}, schema=schema))


def test_session_context_provider_port() -> None:
    """Session-context provider port implementations expose session_context."""
    provider = cast("SessionContextProviderPort", _SessionContextProviderImpl())
    assert provider.session_context() is not None
