"""Tests for core and extended extraction schema helpers."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.schema.extraction_schemas_core import core_extraction_schemas
from datafusion_engine.schema.extraction_schemas_extended import extended_extraction_schemas


def test_core_extraction_schemas_returns_arrow_schemas() -> None:
    """Core extraction schema helper returns Arrow schema payloads."""
    payload = core_extraction_schemas()
    assert "ast_files_v1" in payload
    assert isinstance(payload["ast_files_v1"], pa.Schema)


def test_extended_extraction_schemas_returns_arrow_schemas() -> None:
    """Extended extraction schema helper returns Arrow schema payloads."""
    payload = extended_extraction_schemas()
    assert "libcst_files_v1" in payload
    assert isinstance(payload["libcst_files_v1"], pa.Schema)
