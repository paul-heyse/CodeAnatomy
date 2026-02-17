"""Unit tests for relspec port protocols."""

from __future__ import annotations

from typing import cast

from relspec.ports import DatasetSpecProvider, LineagePort


class _LineageImpl:
    @staticmethod
    def extract_lineage(plan: object) -> object:
        return {"plan": plan}

    @staticmethod
    def resolve_required_udfs(bundle: object) -> frozenset[str]:
        _ = bundle
        return frozenset({"udf_a"})


class _DatasetSpecProviderImpl:
    @staticmethod
    def extract_dataset_spec(value: object) -> object | None:
        return value

    @staticmethod
    def normalize_dataset_spec(value: object) -> object | None:
        return value


def test_lineage_port_protocol() -> None:
    """Lineage protocol implementation satisfies required methods."""
    lineage = cast("LineagePort", _LineageImpl())
    assert lineage.extract_lineage("plan") == {"plan": "plan"}
    assert lineage.resolve_required_udfs({}) == frozenset({"udf_a"})


def test_dataset_spec_provider_protocol() -> None:
    """Dataset spec provider protocol implementation satisfies required methods."""
    provider = cast("DatasetSpecProvider", _DatasetSpecProviderImpl())
    assert provider.extract_dataset_spec({"x": 1}) == {"x": 1}
    assert provider.normalize_dataset_spec({"x": 1}) == {"x": 1}
