"""Tests for metadata read adapter helpers."""

from __future__ import annotations

from typing import cast

import pytest

from datafusion_engine.arrow import metadata_read
from datafusion_engine.arrow.interop import SchemaLike


def test_read_metadata_payload_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Read helper delegates to metadata_payload implementation."""
    monkeypatch.setattr(metadata_read, "metadata_payload", lambda _schema: {"a": 1})
    assert metadata_read.read_metadata_payload(cast("SchemaLike", object())) == {"a": 1}
