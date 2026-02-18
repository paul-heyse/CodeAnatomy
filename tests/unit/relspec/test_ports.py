"""Unit tests for relspec port protocols."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from relspec.ports import DatasetSpecProvider, LineagePort, RuntimeProfilePort


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


@dataclass(frozen=True)
class _PoliciesImpl:
    write_policy: object | None = None


@dataclass(frozen=True)
class _FeaturesImpl:
    enable_delta_cdf: bool = False


@dataclass(frozen=True)
class _RuntimeProfileImpl:
    policies: _PoliciesImpl = _PoliciesImpl(write_policy={"mode": "delta"})
    features: _FeaturesImpl = _FeaturesImpl(enable_delta_cdf=True)


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


def test_runtime_profile_port_protocol() -> None:
    """Runtime profile protocol exposes policy and feature projections."""
    profile = cast("RuntimeProfilePort", _RuntimeProfileImpl())
    assert profile.policies.write_policy is not None
    assert profile.features.enable_delta_cdf is True
