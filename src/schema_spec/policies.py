"""Schema policies for engine integrations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class DataFusionWritePolicy:
    """Configuration for DataFusion write options."""

    partition_by: Sequence[str] = ()
    single_file_output: bool = False
    sort_by: Sequence[str] = ()
    parquet_compression: str | None = None
    parquet_statistics_enabled: str | None = None
    parquet_row_group_size: int | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for the write policy.

        Returns
        -------
        dict[str, object]
            Serialized write policy payload.
        """
        return {
            "partition_by": list(self.partition_by),
            "single_file_output": self.single_file_output,
            "sort_by": list(self.sort_by),
            "parquet_compression": self.parquet_compression,
            "parquet_statistics_enabled": self.parquet_statistics_enabled,
            "parquet_row_group_size": self.parquet_row_group_size,
        }


__all__ = ["DataFusionWritePolicy"]
