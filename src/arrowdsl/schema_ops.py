"""Schema transformation helpers built on Arrow alignment."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.pyarrow_protocols import SchemaLike, TableLike
from arrowdsl.schema import AlignmentInfo, CastErrorPolicy, align_to_schema


@dataclass(frozen=True)
class SchemaTransform:
    """Schema alignment transform using shared alignment utilities.

    Parameters
    ----------
    schema:
        Target schema.
    safe_cast:
        When ``True``, allow safe casts only.
    keep_extra_columns:
        When ``True``, retain extra columns after alignment.
    """

    schema: SchemaLike
    safe_cast: bool = True
    keep_extra_columns: bool = False
    on_error: CastErrorPolicy = "unsafe"

    def apply(self, table: TableLike) -> TableLike:
        """Align a table to the stored schema.

        Parameters
        ----------
        table:
            Input table.

        Returns
        -------
        TableLike
            Aligned table.
        """
        aligned, _ = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned

    def apply_with_info(self, table: TableLike) -> tuple[TableLike, AlignmentInfo]:
        """Align a table to the stored schema and return alignment info.

        Parameters
        ----------
        table:
            Input table.

        Returns
        -------
        tuple[TableLike, AlignmentInfo]
            Aligned table and alignment metadata.
        """
        aligned, info = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned, info
