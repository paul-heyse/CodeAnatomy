"""Schema policies for engine integrations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from core.config_base import FingerprintableConfig, config_fingerprint


@dataclass(frozen=True)
class DataFusionWritePolicy(FingerprintableConfig):
    """Configuration for DataFusion write options."""

    partition_by: Sequence[str] = ()
    single_file_output: bool = False
    sort_by: Sequence[str] = ()

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the write policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing write policy settings.
        """
        return {
            "partition_by": tuple(self.partition_by),
            "single_file_output": self.single_file_output,
            "sort_by": tuple(self.sort_by),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the write policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())

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
