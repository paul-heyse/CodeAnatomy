# ruff: noqa: D100, D103, ANN001, ARG005
from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.dataset import table_registration
from datafusion_engine.dataset.registration_core import DataFusionCachePolicy
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_register_dataset_table_delegates(monkeypatch) -> None:
    monkeypatch.setattr(table_registration, "register_dataset_df", lambda *args, **kwargs: "df")

    result = table_registration.register_dataset_table(
        SessionContext(),
        name="dataset",
        location=DatasetLocation(path="/tmp/example", format="parquet"),
        runtime_profile=DataFusionRuntimeProfile(),
        cache_policy=DataFusionCachePolicy(enabled=True),
        overwrite=False,
    )

    assert result == "df"
