# ruff: noqa: D100, D103, ANN001
from __future__ import annotations

from datafusion_engine.dataset import schema_registration
from datafusion_engine.dataset.registry import DatasetLocation


def test_resolved_registration_schema_delegates(monkeypatch) -> None:
    location = DatasetLocation(path="/tmp/example", format="parquet")

    monkeypatch.setattr(schema_registration, "resolve_dataset_schema", lambda _loc: "schema")

    assert schema_registration.resolved_registration_schema(location) == "schema"
