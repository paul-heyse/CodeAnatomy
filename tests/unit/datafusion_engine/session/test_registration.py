"""Tests for dataset registration delegation from session facade."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from datafusion import SessionContext

from datafusion_engine.dataset.registration_core import DataFusionCachePolicy
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.registration import register_dataset_df
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass
class _FakeFacade:
    called: bool = False

    def register_dataset_df(
        self,
        *,
        name: str,
        location: DatasetLocation,
        cache_policy: DataFusionCachePolicy,
        overwrite: bool,
    ) -> str:
        self.called = True
        assert name == "dataset"
        assert isinstance(location, DatasetLocation)
        assert isinstance(cache_policy, DataFusionCachePolicy)
        assert overwrite is False
        return "ok"


def test_register_dataset_df_delegates_to_registry_facade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public registration helper delegates to runtime registry facade."""
    ctx = SessionContext()
    profile = DataFusionRuntimeProfile()
    facade = _FakeFacade()

    def _fake_registry_facade_for_context(*_args: object, **_kwargs: object) -> _FakeFacade:
        return facade

    monkeypatch.setattr(
        "datafusion_engine.session.registration.registry_facade_for_context",
        _fake_registry_facade_for_context,
    )

    result = register_dataset_df(
        ctx,
        profile=profile,
        name="dataset",
        location=DatasetLocation(path="/tmp/example", format="parquet"),
        cache_policy=DataFusionCachePolicy(enabled=True),
        overwrite=False,
    )

    assert facade.called is True
    assert result == "ok"
