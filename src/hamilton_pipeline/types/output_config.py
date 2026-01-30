"""Output configuration types for the Hamilton pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from engine.plan_policy import WriterStrategy
    from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
    from storage.ipc_utils import IpcWriteConfig


OutputStorageFormat = Literal["delta"]


@dataclass(frozen=True)
class OutputStoragePolicy:
    """Configuration for output storage format enforcement."""

    format: OutputStorageFormat = "delta"
    allow_parquet_exports: bool = False


@dataclass(frozen=True)
class OutputConfig:
    """Configuration for output and intermediate materialization."""

    work_dir: str | None
    output_dir: str | None
    overwrite_intermediate_datasets: bool
    materialize_param_tables: bool = False
    writer_strategy: WriterStrategy = "arrow"
    ipc_dump_enabled: bool = False
    ipc_write_config: IpcWriteConfig | None = None
    output_storage_policy: OutputStoragePolicy = field(default_factory=OutputStoragePolicy)
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    delta_storage_options: Mapping[str, str] | None = None


__all__ = [
    "OutputConfig",
    "OutputStorageFormat",
    "OutputStoragePolicy",
]
