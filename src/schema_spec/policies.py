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
        }


__all__ = ["DataFusionWritePolicy"]
