"""Output configuration types for the Hamilton pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from core.config_base import FingerprintableConfig, config_fingerprint

if TYPE_CHECKING:
    from datafusion_engine.materialize_policy import WriterStrategy
    from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
    from storage.ipc_utils import IpcWriteConfig


OutputStorageFormat = Literal["delta"]


@dataclass(frozen=True)
class OutputStoragePolicy(FingerprintableConfig):
    """Configuration for output storage format enforcement."""

    format: OutputStorageFormat = "delta"
    allow_parquet_exports: bool = False

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for output storage policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing output storage policy settings.
        """
        return {
            "format": self.format,
            "allow_parquet_exports": self.allow_parquet_exports,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for output storage policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class OutputConfig:
    """Configuration for output and intermediate materialization."""

    work_dir: str | None
    output_dir: str | None
    overwrite_intermediate_datasets: bool
    semantic_output_catalog_name: str | None = None
    extract_output_catalog_name: str | None = None
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
