"""Guards for rust-native Delta write cutover in write pipeline."""

from __future__ import annotations

from pathlib import Path


def test_write_pipeline_has_no_direct_write_deltalake_calls() -> None:
    """Write pipeline should not call Python write_deltalake directly."""
    source = Path("src/datafusion_engine/io/write_pipeline.py").read_text(encoding="utf-8")
    assert "write_deltalake(" not in source
