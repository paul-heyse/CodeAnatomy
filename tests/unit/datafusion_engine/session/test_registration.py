# ruff: noqa: D100, D103, ANN001, ANN002, ANN003, ANN202, INP001
from __future__ import annotations

from dataclasses import dataclass

from datafusion import SessionContext

from datafusion_engine.dataset.registration import DataFusionCachePolicy
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.registration import register_dataset_df
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass
class _FakeFacade:
    called: bool = False

    def register_dataset_df(self, *, name, location, cache_policy, overwrite):
        self.called = True
        assert name == "dataset"
        assert isinstance(location, DatasetLocation)
        assert isinstance(cache_policy, DataFusionCachePolicy)
        assert overwrite is False
        return "ok"


def test_register_dataset_df_delegates_to_registry_facade(monkeypatch) -> None:
    ctx = SessionContext()
    profile = DataFusionRuntimeProfile()
    facade = _FakeFacade()

    def _fake_registry_facade_for_context(*_args, **_kwargs):
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
