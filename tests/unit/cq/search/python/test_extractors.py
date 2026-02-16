"""Tests for python extractor wrappers."""

from __future__ import annotations

from typing import cast

import pytest
from tools.cq.search._shared.core import (
    PythonByteRangeEnrichmentRequest,
    PythonNodeEnrichmentRequest,
)
from tools.cq.search.python import extractors as extractors_module


def test_extract_python_node_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test extract python node delegates."""
    monkeypatch.setattr(
        extractors_module,
        "enrich_python_context",
        lambda request: {"ok": True, "request": request},
    )
    payload = extractors_module.extract_python_node(cast("PythonNodeEnrichmentRequest", object()))
    assert payload["ok"] is True


def test_extract_python_byte_range_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test extract python byte range delegates."""
    monkeypatch.setattr(
        extractors_module,
        "enrich_python_context_by_byte_range",
        lambda request: {"range": True, "request": request},
    )
    payload = extractors_module.extract_python_byte_range(
        cast("PythonByteRangeEnrichmentRequest", object())
    )
    assert payload["range"] is True
