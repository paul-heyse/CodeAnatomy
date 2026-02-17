"""Tests for dataset table registration adapter."""

from __future__ import annotations

import pytest
from datafusion import SessionContext

from datafusion_engine.dataset import table_registration
from datafusion_engine.dataset.registration_core import DataFusionCachePolicy
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_register_dataset_table_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register dataset table delegates to register_dataset_df."""

    def _fake_register_dataset_df(*_unused: object, **_unused_kwargs: object) -> str:
        return "df"

    monkeypatch.setattr(table_registration, "register_dataset_df", _fake_register_dataset_df)

    result = table_registration.register_dataset_table(
        SessionContext(),
        name="dataset",
        location=DatasetLocation(path="/tmp/example", format="parquet"),
        runtime_profile=DataFusionRuntimeProfile(),
        cache_policy=DataFusionCachePolicy(enabled=True),
        overwrite=False,
    )

    assert result == "df"
