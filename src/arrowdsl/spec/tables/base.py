"""Base codec helpers for Arrow spec tables."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from pyarrow import ipc

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.validation import ArrowValidationOptions, ValidationReport
from arrowdsl.spec.io import (
    ipc_read_options_factory,
    ipc_write_options_factory,
    read_spec_table,
    rows_from_table,
    sort_spec_table,
    table_from_json,
    table_from_json_file,
    table_from_rows,
    write_spec_table,
)

if TYPE_CHECKING:
    from arrowdsl.spec.infra import SpecTableSpec


@dataclass(frozen=True)
class SpecTableCodec[SpecT]:
    """Codec for Arrow spec tables."""

    schema: SchemaLike
    spec: SpecTableSpec
    encode_row: Callable[[SpecT], dict[str, object]]
    decode_row: Callable[[Mapping[str, object]], SpecT]
    sort_keys: tuple[str, ...] = ()

    def to_table(self, values: Sequence[SpecT]) -> pa.Table:
        """Encode spec values into a table.

        Returns
        -------
        pa.Table
            Table built from encoded spec rows.
        """
        rows = [self.encode_row(value) for value in values]
        table = table_from_rows(self.schema, rows)
        if not self.sort_keys:
            return table
        return sort_spec_table(table, keys=self.sort_keys)

    def from_table(self, table: pa.Table) -> tuple[SpecT, ...]:
        """Decode spec values from a table.

        Returns
        -------
        tuple[SpecT, ...]
            Spec values decoded from the table.
        """
        return tuple(self.decode_row(row) for row in rows_from_table(table))

    def to_json_payload(self, values: Sequence[SpecT]) -> list[dict[str, object]]:
        """Encode spec values into JSON-ready rows.

        Returns
        -------
        list[dict[str, object]]
            JSON-ready payload rows.
        """
        return [self.encode_row(value) for value in values]

    def table_from_json_payload(
        self,
        payload: Sequence[Mapping[str, object]],
    ) -> pa.Table:
        """Build a spec table from JSON-ready rows.

        Returns
        -------
        pa.Table
            Table built from JSON-ready rows.
        """
        rows = [dict(item) for item in payload]
        return table_from_json(self.schema, rows)

    def table_from_json_file(self, path: str | Path) -> pa.Table:
        """Build a spec table from a JSON file.

        Returns
        -------
        pa.Table
            Table built from a JSON file.
        """
        return table_from_json_file(self.schema, path)

    def from_json_payload(
        self,
        payload: Sequence[Mapping[str, object]],
    ) -> tuple[SpecT, ...]:
        """Decode spec values from JSON-ready rows.

        Returns
        -------
        tuple[SpecT, ...]
            Spec values decoded from JSON-ready rows.
        """
        return self.from_table(self.table_from_json_payload(payload))

    def from_json_file(self, path: str | Path) -> tuple[SpecT, ...]:
        """Decode spec values from a JSON file.

        Returns
        -------
        tuple[SpecT, ...]
            Spec values decoded from the JSON file.
        """
        return self.from_table(self.table_from_json_file(path))

    @staticmethod
    def write_table(
        path: str | Path,
        table: pa.Table,
        *,
        options: ipc.IpcWriteOptions | None = None,
    ) -> None:
        """Write a spec table to IPC."""
        write_spec_table(path, table, options=options or ipc_write_options_factory())

    @staticmethod
    def read_table(
        path: str | Path,
        *,
        options: ipc.IpcReadOptions | None = None,
    ) -> pa.Table:
        """Read a spec table from IPC.

        Returns
        -------
        pa.Table
            Table loaded from IPC.
        """
        return read_spec_table(path, options=options or ipc_read_options_factory())

    def write_values(
        self,
        path: str | Path,
        values: Sequence[SpecT],
        *,
        options: ipc.IpcWriteOptions | None = None,
    ) -> None:
        """Write encoded spec values to IPC."""
        self.write_table(path, self.to_table(values), options=options)

    def read_values(
        self,
        path: str | Path,
        *,
        options: ipc.IpcReadOptions | None = None,
    ) -> tuple[SpecT, ...]:
        """Read encoded spec values from IPC.

        Returns
        -------
        tuple[SpecT, ...]
            Spec values decoded from the table.
        """
        return self.from_table(self.read_table(path, options=options))

    def align(self, table: pa.Table) -> pa.Table:
        """Align a table to the spec schema.

        Returns
        -------
        pa.Table
            Table aligned to the spec schema.
        """
        return self.spec.align(table)

    def validate(
        self,
        table: pa.Table,
        *,
        ctx: ExecutionContext,
        options: ArrowValidationOptions | None = None,
    ) -> ValidationReport:
        """Validate a table against the spec schema.

        Returns
        -------
        ValidationReport
            Validation report for the table.
        """
        return self.spec.validate(table, ctx=ctx, options=options)


__all__ = ["SpecTableCodec"]
