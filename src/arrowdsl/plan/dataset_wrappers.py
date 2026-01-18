"""Dataset wrappers for one-shot and composite sources."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.dataset as ds


@dataclass
class OneShotDataset:
    """Wrap a dataset to enforce single-scan semantics."""

    dataset: ds.Dataset
    reader: pa.RecordBatchReader | None = None
    _scanned: bool = field(default=False, init=False, repr=False)

    @property
    def schema(self) -> pa.Schema:
        """Return the dataset schema.

        Returns
        -------
        pyarrow.Schema
            Arrow schema for the dataset.
        """
        if self.reader is not None:
            return self.reader.schema
        return self.dataset.schema

    @classmethod
    def from_reader(cls, reader: pa.RecordBatchReader) -> OneShotDataset:
        """Create a one-shot wrapper around a record batch reader.

        Returns
        -------
        OneShotDataset
            Wrapper that enforces single-scan semantics.
        """
        dataset = ds.dataset(reader, schema=reader.schema)
        return cls(dataset=dataset, reader=reader)

    def consume(self) -> ds.Dataset:
        """Return the wrapped dataset, enforcing single-use semantics.

        Returns
        -------
        pyarrow.dataset.Dataset
            The wrapped dataset.

        Raises
        ------
        ValueError
            Raised when the dataset has already been scanned.
        """
        if self._scanned:
            msg = "One-shot dataset has already been scanned."
            raise ValueError(msg)
        self._scanned = True
        return self.dataset

    def scanner(self, *args: object, **kwargs: object) -> ds.Scanner:
        """Return a scanner while enforcing single-use semantics.

        Returns
        -------
        pyarrow.dataset.Scanner
            Scanner for the wrapped dataset.

        Raises
        ------
        ValueError
            Raised when the dataset has already been scanned.
        """
        if self._scanned:
            msg = "One-shot dataset has already been scanned."
            raise ValueError(msg)
        self._scanned = True
        if self.reader is None:
            return self.dataset.scanner(*args, **kwargs)
        return ds.Scanner.from_batches(self.reader, schema=self.schema, **kwargs)

    def __getattr__(self, name: str) -> object:
        """Delegate missing attributes to the underlying dataset.

        Returns
        -------
        object
            Attribute from the underlying dataset.
        """
        return getattr(self.dataset, name)


DatasetLike = ds.Dataset | OneShotDataset


def unwrap_dataset(dataset: DatasetLike) -> ds.Dataset:
    """Return a dataset, consuming one-shot wrappers when needed.

    Returns
    -------
    pyarrow.dataset.Dataset
        Dataset ready for scanning.
    """
    if isinstance(dataset, OneShotDataset):
        return dataset.consume()
    return dataset


def union_dataset(datasets: Sequence[ds.Dataset]) -> ds.Dataset:
    """Return a UnionDataset over multiple datasets.

    Returns
    -------
    pyarrow.dataset.Dataset
        Composite dataset spanning all inputs.
    """
    return ds.dataset(list(datasets))


def is_one_shot_dataset(value: object) -> bool:
    """Return whether the value is a one-shot dataset wrapper.

    Returns
    -------
    bool
        True when the value is a one-shot dataset.
    """
    return isinstance(value, OneShotDataset)


__all__ = [
    "DatasetLike",
    "OneShotDataset",
    "is_one_shot_dataset",
    "union_dataset",
    "unwrap_dataset",
]
