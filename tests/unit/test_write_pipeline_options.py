"""Unit tests for write pipeline option encoding."""

from __future__ import annotations

from datafusion_engine.write_pipeline import ParquetWritePolicy, _parquet_column_options

COMPRESSION_LEVEL = 5
DATA_PAGE_SIZE = 8192
BLOOM_FILTER_NDV = 12


def test_parquet_policy_dataset_options() -> None:
    """Emit dataset options for parquet policies."""
    policy = ParquetWritePolicy(
        compression="zstd",
        compression_level=COMPRESSION_LEVEL,
        data_page_size=DATA_PAGE_SIZE,
        dictionary_enabled=False,
        statistics_enabled="page",
    )
    options = policy.to_dataset_options()
    assert options["compression"] == "zstd"
    assert options["compression_level"] == COMPRESSION_LEVEL
    assert options["data_page_size"] == DATA_PAGE_SIZE
    assert options["use_dictionary"] is False
    assert options["write_statistics"] is True


def test_parquet_column_options_include_overrides() -> None:
    """Encode per-column overrides for parquet writers."""
    overrides = {
        "id": {
            "compression": "snappy",
            "dictionary_enabled": "true",
            "bloom_filter_ndv": str(BLOOM_FILTER_NDV),
        }
    }
    resolved = _parquet_column_options(overrides)
    options = resolved["id"]
    assert options.compression == "snappy"
    assert options.dictionary_enabled is True
    assert options.bloom_filter_ndv == BLOOM_FILTER_NDV
