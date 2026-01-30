"""Delta Lake policy helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated, Literal

import msgspec

from core.config_base import config_fingerprint
from serde_msgspec import StructBaseStrict

_COLUMN_NAME_PATTERN = "^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$"

NonNegInt = Annotated[
    int,
    msgspec.Meta(
        ge=0,
        title="Non-negative Integer",
        description="Non-negative integer value.",
        examples=[0, 32],
    ),
]
PositiveInt = Annotated[
    int,
    msgspec.Meta(
        ge=1,
        title="Positive Integer",
        description="Positive integer value.",
        examples=[1, 128],
    ),
]
PositiveFloat = Annotated[
    float,
    msgspec.Meta(
        gt=0,
        title="Positive Float",
        description="Positive floating-point value.",
        examples=[0.01, 1.0],
    ),
]
NonEmptyStr = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        title="Non-empty String",
        description="Non-empty string value.",
        examples=["value"],
    ),
]
ColumnName = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        max_length=128,
        pattern=_COLUMN_NAME_PATTERN,
        title="Column Name",
        description="Column identifier used in Delta and Parquet policies.",
        examples=["node_id", "repo_name"],
    ),
]
StatsPolicy = Annotated[
    Literal["off", "explicit", "auto"],
    msgspec.Meta(
        title="Stats Policy",
        description="Stats collection policy for Delta writes.",
        examples=["auto"],
    ),
]
SchemaMode = Annotated[
    Literal["merge", "overwrite"],
    msgspec.Meta(
        title="Schema Mode",
        description="Schema evolution mode for Delta writes.",
        examples=["merge"],
    ),
]
ColumnMappingMode = Annotated[
    Literal["id", "name"],
    msgspec.Meta(
        title="Column Mapping Mode",
        description="Delta column mapping mode.",
        examples=["name"],
    ),
]


class DeltaWritePolicy(StructBaseStrict, frozen=True):
    """Configuration for Delta write behavior."""

    target_file_size: PositiveInt | None = 96 * 1024 * 1024
    partition_by: tuple[ColumnName, ...] = ()
    zorder_by: tuple[ColumnName, ...] = ()
    stats_policy: StatsPolicy = "auto"
    stats_columns: tuple[ColumnName, ...] | None = None
    stats_max_columns: NonNegInt = 32
    parquet_writer_policy: ParquetWriterPolicy | None = None
    enable_features: tuple[
        Literal[
            "change_data_feed",
            "deletion_vectors",
            "row_tracking",
            "in_commit_timestamps",
            "column_mapping",
            "v2_checkpoints",
        ],
        ...,
    ] = ()

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return canonical payload for fingerprinting.

        Returns
        -------
        Mapping[str, object]
            Payload used for policy fingerprinting.
        """
        return {
            "target_file_size": self.target_file_size,
            "partition_by": self.partition_by,
            "zorder_by": self.zorder_by,
            "stats_policy": self.stats_policy,
            "stats_columns": self.stats_columns,
            "stats_max_columns": self.stats_max_columns,
            "parquet_writer_policy": (
                self.parquet_writer_policy.fingerprint_payload()
                if self.parquet_writer_policy is not None
                else None
            ),
            "enable_features": self.enable_features,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the policy.

        Returns
        -------
        str
            Fingerprint string for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


class ParquetWriterPolicy(StructBaseStrict, frozen=True):
    """Per-column Parquet writer settings for Delta writes."""

    statistics_enabled: tuple[ColumnName, ...] = ()
    statistics_level: Literal["none", "chunk", "page"] = "page"
    bloom_filter_enabled: tuple[ColumnName, ...] = ()
    bloom_filter_fpp: PositiveFloat | None = None
    bloom_filter_ndv: NonNegInt | None = None
    dictionary_enabled: tuple[ColumnName, ...] = ()

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return canonical payload for fingerprinting.

        Returns
        -------
        Mapping[str, object]
            Payload used for policy fingerprinting.
        """
        return {
            "statistics_enabled": self.statistics_enabled,
            "statistics_level": self.statistics_level,
            "bloom_filter_enabled": self.bloom_filter_enabled,
            "bloom_filter_fpp": self.bloom_filter_fpp,
            "bloom_filter_ndv": self.bloom_filter_ndv,
            "dictionary_enabled": self.dictionary_enabled,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the policy.

        Returns
        -------
        str
            Fingerprint string for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


class DeltaSchemaPolicy(StructBaseStrict, frozen=True):
    """Schema evolution and column mapping policy for Delta."""

    schema_mode: SchemaMode | None = None
    column_mapping_mode: ColumnMappingMode | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return canonical payload for fingerprinting.

        Returns
        -------
        Mapping[str, object]
            Payload used for policy fingerprinting.
        """
        return {
            "schema_mode": self.schema_mode,
            "column_mapping_mode": self.column_mapping_mode,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the policy.

        Returns
        -------
        str
            Fingerprint string for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


class StatsColumnsInputs(StructBaseStrict, frozen=True):
    """Inputs for resolving Delta stats columns."""

    policy: DeltaWritePolicy | None
    partition_by: Sequence[ColumnName] = ()
    zorder_by: Sequence[ColumnName] = ()
    extra_candidates: Sequence[ColumnName] = ()
    schema_columns: Sequence[ColumnName] | None = None
    override: Sequence[ColumnName] | None = None


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
    return config or None


def resolve_stats_columns(inputs: StatsColumnsInputs) -> tuple[str, ...] | None:
    """Resolve Delta stats columns based on policy and schema.

    Returns
    -------
    tuple[str, ...] | None
        Resolved stats columns, or ``None`` when disabled.
    """
    if inputs.override is not None:
        cols = list(dict.fromkeys(inputs.override))
    elif inputs.policy is None or inputs.policy.stats_policy == "off":
        return None
    elif inputs.policy.stats_policy == "explicit":
        if inputs.policy.stats_columns is None:
            return None
        cols = list(dict.fromkeys(inputs.policy.stats_columns))
    else:
        cols = list(
            dict.fromkeys([*inputs.partition_by, *inputs.zorder_by, *inputs.extra_candidates])
        )
        cols = cols[: inputs.policy.stats_max_columns]

    if inputs.schema_columns is not None:
        schema_set = set(inputs.schema_columns)
        cols = [col for col in cols if col in schema_set]
    return tuple(cols)


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
    "ParquetWriterPolicy",
    "StatsColumnsInputs",
    "delta_schema_configuration",
    "delta_write_configuration",
    "resolve_stats_columns",
]
