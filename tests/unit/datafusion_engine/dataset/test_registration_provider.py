"""Tests for registration provider helpers."""

from __future__ import annotations

import pytest

from datafusion_engine.dataset.registration_provider import (
    provider_capsule_id,
    table_provider_capsule,
)


class _ProviderWithCapsuleMethod:
    def __init__(self) -> None:
        def _provider_capsule() -> object:
            return object()

        self.__datafusion_table_provider__ = _provider_capsule


class _ProviderWithPropertyMethod:
    @staticmethod
    def datafusion_table_provider() -> object:
        return "capsule"


class _ProviderWithoutCapsule:
    pass


def test_table_provider_capsule_prefers_dunder_capsule() -> None:
    """Prefer dunder capsule hook when provider exposes both capsule styles."""
    capsule = table_provider_capsule(_ProviderWithCapsuleMethod())
    assert capsule is not None


def test_table_provider_capsule_falls_back_to_property_method() -> None:
    """Fallback to property-style capsule hook when dunder hook is absent."""
    assert table_provider_capsule(_ProviderWithPropertyMethod()) == "capsule"


def test_provider_capsule_id_requires_capsule() -> None:
    """Raise when provider does not expose any supported capsule hook."""
    with pytest.raises(AttributeError):
        provider_capsule_id(_ProviderWithoutCapsule())
