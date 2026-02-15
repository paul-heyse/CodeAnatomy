"""Tests for runtime-to-contract conversion helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search._shared.core import PythonNodeEnrichmentSettingsV1, convert_from_attributes


@dataclass(frozen=True)
class _RuntimeAttrs:
    source_bytes: bytes
    line: int
    col: int
    cache_key: str
    byte_start: int | None = None
    byte_end: int | None = None
    query_budget_ms: int | None = None


def test_convert_from_attributes_to_msgspec_struct() -> None:
    runtime = _RuntimeAttrs(
        source_bytes=b"x",
        line=1,
        col=0,
        cache_key="abc",
        query_budget_ms=10,
    )
    converted = convert_from_attributes(runtime, type_=PythonNodeEnrichmentSettingsV1)
    assert isinstance(converted, PythonNodeEnrichmentSettingsV1)
    assert converted.cache_key == "abc"
    assert converted.query_budget_ms == 10
