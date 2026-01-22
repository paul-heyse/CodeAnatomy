"""Schema policies for engine integrations."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class ParquetColumnPolicy:
    """Configuration overrides for a single Parquet column."""

    compression: str | None = None
    dictionary_enabled: bool | None = None
    statistics_enabled: str | None = None
    bloom_filter_enabled: bool | None = None
    bloom_filter_fpp: float | None = None
    bloom_filter_ndv: int | None = None
    encoding: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for the column policy.

        Returns
        -------
        dict[str, object]
            Serialized column policy payload.
        """
        return {
            "compression": self.compression,
            "dictionary_enabled": self.dictionary_enabled,
            "statistics_enabled": self.statistics_enabled,
            "bloom_filter_enabled": self.bloom_filter_enabled,
            "bloom_filter_fpp": self.bloom_filter_fpp,
            "bloom_filter_ndv": self.bloom_filter_ndv,
            "encoding": self.encoding,
        }


@dataclass(frozen=True)
class DataFusionWritePolicy:
    """Configuration for DataFusion write options."""

    partition_by: Sequence[str] = ()
    single_file_output: bool = False
    sort_by: Sequence[str] = ()
    parquet_compression: str | None = None
    parquet_statistics_enabled: str | None = None
    parquet_row_group_size: int | None = None
    parquet_bloom_filter_on_write: bool | None = None
    parquet_dictionary_enabled: bool | None = None
    parquet_encoding: str | None = None
    parquet_skip_arrow_metadata: bool | None = None
    parquet_column_options: Mapping[str, ParquetColumnPolicy] | None = None

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
            "parquet_bloom_filter_on_write": self.parquet_bloom_filter_on_write,
            "parquet_dictionary_enabled": self.parquet_dictionary_enabled,
            "parquet_encoding": self.parquet_encoding,
            "parquet_skip_arrow_metadata": self.parquet_skip_arrow_metadata,
            "parquet_column_options": _column_option_payloads(self.parquet_column_options),
        }


def _column_option_payloads(
    options: Mapping[str, ParquetColumnPolicy] | None,
) -> Mapping[str, str] | None:
    if not options:
        return None
    payloads: dict[str, str] = {}
    for name, option in options.items():
        compact = {key: value for key, value in option.payload().items() if value is not None}
        payloads[name] = json.dumps(compact, sort_keys=True)
    return payloads


__all__ = ["DataFusionWritePolicy", "ParquetColumnPolicy"]
