"""Contract tests for registry snapshot normalization."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.udf.extension_core import ExtensionRegistries, _normalize_registry_snapshot

if TYPE_CHECKING:
    from datafusion import SessionContext


def _registries() -> ExtensionRegistries:
    return ExtensionRegistries()


def _ctx() -> SessionContext:
    return cast("SessionContext", object())


def test_registry_snapshot_contract_defaults_version() -> None:
    """Normalizer should default missing snapshot version to v1."""
    snapshot = _normalize_registry_snapshot(
        {"scalar": ["stable_id"]},
        ctx=_ctx(),
        registries=_registries(),
    )
    assert snapshot["version"] == 1


def test_registry_snapshot_contract_rejects_unsupported_version() -> None:
    """Unsupported snapshot versions should raise ValueError."""
    with pytest.raises(ValueError, match="unsupported"):
        _normalize_registry_snapshot(
            {"version": 2, "scalar": ["stable_id"]},
            ctx=_ctx(),
            registries=_registries(),
        )


def test_registry_snapshot_contract_rejects_non_integer_version() -> None:
    """Non-integer snapshot versions should raise TypeError."""
    with pytest.raises(TypeError, match="must be an integer"):
        _normalize_registry_snapshot(
            {"version": "1", "scalar": ["stable_id"]},
            ctx=_ctx(),
            registries=_registries(),
        )
