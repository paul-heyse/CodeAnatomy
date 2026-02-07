"""Unit tests for resolver tracking in Delta CDF registration."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.delta import cdf as cdf_module

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver


def test_register_cdf_inputs_records_resolver_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CDF registration should record resolver identity at the boundary."""
    calls: list[tuple[object, str]] = []
    resolver = object()

    monkeypatch.setattr(
        "semantics.resolver_identity.record_resolver_if_tracking",
        lambda tracked_resolver, *, label="unknown": calls.append((tracked_resolver, label)),
    )

    class _Adapter:
        def __init__(self, **_kwargs: object) -> None:
            return None

    class _Facade:
        def __init__(self, **_kwargs: object) -> None:
            return None

    monkeypatch.setattr(cdf_module, "DataFusionIOAdapter", _Adapter)
    monkeypatch.setattr(cdf_module, "DataFusionExecutionFacade", _Facade)

    result = cdf_module.register_cdf_inputs(
        cast("SessionContext", object()),
        cast("DataFusionRuntimeProfile", object()),
        table_names=(),
        dataset_resolver=cast("ManifestDatasetResolver", resolver),
    )

    assert result == {}
    assert calls == [(resolver, "cdf_registration")]
