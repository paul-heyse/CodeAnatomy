"""Hamilton data adapters for pyarrow table caching."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from functools import lru_cache

import pyarrow as pa
from hamilton.io.data_adapters import DataLoader, DataSaver
from hamilton.io.utils import get_file_metadata
from hamilton.registry import register_adapter

from storage.parquet import read_table_parquet, write_table_parquet


@dataclass
class ArrowParquetLoader(DataLoader):
    """Load a pyarrow.Table from a Parquet file."""

    path: str

    @classmethod
    def applicable_types(cls) -> Collection[type]:
        """Return the types this loader can produce.

        Returns
        -------
        Collection[type]
            Supported output types.
        """
        return [pa.Table]

    @classmethod
    def name(cls) -> str:
        """Return the adapter name used by Hamilton.

        Returns
        -------
        str
            Adapter name used for registration.
        """
        return "parquet"

    def load_data(self, type_: type) -> tuple[pa.Table, dict[str, object]]:
        """Load a pyarrow.Table and file metadata from parquet.

        Returns
        -------
        tuple[pyarrow.Table, dict[str, object]]
            Loaded table and metadata.
        """
        _ = type_
        table = read_table_parquet(self.path)
        return table, get_file_metadata(self.path)


@dataclass
class ArrowParquetSaver(DataSaver):
    """Save a pyarrow.Table to a Parquet file."""

    path: str

    @classmethod
    def applicable_types(cls) -> Collection[type]:
        """Return the types this saver can accept.

        Returns
        -------
        Collection[type]
            Supported input types.
        """
        return [pa.Table]

    @classmethod
    def name(cls) -> str:
        """Return the adapter name used by Hamilton.

        Returns
        -------
        str
            Adapter name used for registration.
        """
        return "parquet"

    def save_data(self, data: pa.Table) -> dict[str, object]:
        """Save the table to parquet and return file metadata.

        Returns
        -------
        dict[str, object]
            File metadata for the saved Parquet file.

        Raises
        ------
        TypeError
            Raised when the input is not a pyarrow.Table.
        """
        if not isinstance(data, pa.Table):
            msg = f"ArrowParquetSaver expected pyarrow.Table, got {type(data)}"
            raise TypeError(msg)
        write_table_parquet(data, self.path)
        return get_file_metadata(self.path)


@lru_cache(maxsize=1)
def register_arrow_parquet_adapters() -> None:
    """Register Arrow parquet adapters with the Hamilton registry."""
    register_adapter(ArrowParquetLoader)
    register_adapter(ArrowParquetSaver)
