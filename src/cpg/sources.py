"""Source wrappers for dataset-backed CPG inputs."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow.dataset as ds

from schema_spec.system import DatasetSpec


@dataclass(frozen=True)
class DatasetSource:
    """Dataset + DatasetSpec pairing for plan compilation."""

    dataset: ds.Dataset
    spec: DatasetSpec


__all__ = ["DatasetSource"]
