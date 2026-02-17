"""Tests for function catalog reader helpers."""

from __future__ import annotations

from datafusion_engine.schema import function_catalog_reader


class _Introspector:
    @staticmethod
    def routines_snapshot() -> list[dict[str, object]]:
        return [{"name": "fn"}]

    @staticmethod
    def parameters_snapshot() -> list[dict[str, object]]:
        return [{"name": "p"}]

    @staticmethod
    def function_catalog_snapshot(*, include_parameters: bool = True) -> list[dict[str, object]]:
        return [{"include_parameters": include_parameters}]


def test_function_catalog_reader_delegates() -> None:
    """Function catalog reader helpers should delegate to introspector methods."""
    i = _Introspector()
    assert function_catalog_reader.routines_snapshot(i) == [{"name": "fn"}]
    assert function_catalog_reader.parameters_snapshot(i) == [{"name": "p"}]
    assert function_catalog_reader.function_catalog_snapshot(i, include_parameters=False) == [
        {"include_parameters": False}
    ]
