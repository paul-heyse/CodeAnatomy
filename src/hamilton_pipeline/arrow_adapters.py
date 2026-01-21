"""Hamilton data adapters for pyarrow table caching."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from functools import lru_cache

import pyarrow as pa
from hamilton.io.data_adapters import DataLoader, DataSaver
from hamilton.registry import register_adapter

from datafusion_engine.runtime import read_delta_table_from_path
from storage.deltalake import (
    DeltaWriteOptions,
    delta_table_version,
    write_table_delta,
)


@dataclass
class ArrowDeltaLoader(DataLoader):
    """Load a pyarrow.Table from a Delta table."""

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
            Adapter name for the loader.
        """
        return "delta"

    def load_data(self, type_: type) -> tuple[pa.Table, dict[str, object]]:
        """Load a pyarrow.Table and file metadata from Delta.

        Returns
        -------
        tuple[pyarrow.Table, dict[str, object]]
            Loaded table and metadata.

        Raises
        ------
        TypeError
            Raised when the Delta reader does not return a pyarrow.Table.
        """
        _ = type_
        table = read_delta_table_from_path(self.path)
        metadata: dict[str, object] = {
            "delta_version": delta_table_version(self.path),
            "path": self.path,
        }
        return table, metadata


@dataclass
class ArrowDeltaSaver(DataSaver):
    """Save a pyarrow.Table to a Delta table."""

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
            Adapter name for the saver.
        """
        return "delta"

    def save_data(self, data: pa.Table) -> dict[str, object]:
        """Save the table to Delta and return file metadata.

        Returns
        -------
        dict[str, object]
            File metadata for the saved Delta table.

        Raises
        ------
        TypeError
            Raised when the input is not a pyarrow.Table.
        """
        if not isinstance(data, pa.Table):
            msg = f"ArrowDeltaSaver expected pyarrow.Table, got {type(data)}"
            raise TypeError(msg)
        result = write_table_delta(
            data,
            self.path,
            options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
        )
        return {"delta_version": result.version, "path": result.path}


@lru_cache(maxsize=1)
def register_arrow_delta_adapters() -> None:
    """Register Arrow delta adapters with the Hamilton registry."""
    register_adapter(ArrowDeltaLoader)
    register_adapter(ArrowDeltaSaver)
