"""Tests for dataset schema registration adapters."""

from __future__ import annotations

import pytest

from datafusion_engine.dataset import schema_registration
from datafusion_engine.dataset.registry import DatasetLocation


def test_resolved_registration_schema_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve dataset schema delegates to canonical resolver."""
    location = DatasetLocation(path="/tmp/example", format="parquet")

    monkeypatch.setattr(schema_registration, "resolve_dataset_schema", lambda _loc: "schema")

    assert schema_registration.resolved_registration_schema(location) == "schema"
