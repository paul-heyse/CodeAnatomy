"""Delta Lake policy helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class DeltaWritePolicy:
    """Configuration for Delta write behavior."""

    target_file_size: int | None = None
    stats_columns: tuple[str, ...] | None = None


@dataclass(frozen=True)
class DeltaSchemaPolicy:
    """Schema evolution and column mapping policy for Delta."""

    schema_mode: Literal["merge", "overwrite"] | None = None
    column_mapping_mode: Literal["id", "name"] | None = None


def delta_write_configuration(policy: DeltaWritePolicy | None) -> dict[str, str] | None:
    """Return Delta table configuration entries for write policies.

    Returns
    -------
    dict[str, str] | None
        Delta configuration values to apply, or ``None`` when no policy exists.
    """
    if policy is None:
        return None
    config: dict[str, str] = {}
    if policy.target_file_size is not None:
        config["delta.targetFileSize"] = str(policy.target_file_size)
    if policy.stats_columns:
        config["delta.dataSkippingStatsColumns"] = ",".join(policy.stats_columns)
    return config or None


def delta_schema_configuration(policy: DeltaSchemaPolicy | None) -> dict[str, str] | None:
    """Return Delta configuration entries for schema policies.

    Returns
    -------
    dict[str, str] | None
        Delta configuration values to apply, or ``None`` when no policy exists.
    """
    if policy is None:
        return None
    config: dict[str, str] = {}
    if policy.column_mapping_mode is not None:
        config["delta.columnMapping.mode"] = policy.column_mapping_mode
    return config or None


__all__ = [
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "delta_schema_configuration",
    "delta_write_configuration",
]
