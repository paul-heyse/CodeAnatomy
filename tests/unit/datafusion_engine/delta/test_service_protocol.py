# ruff: noqa: D103
"""Tests for Delta service protocol contract."""

from __future__ import annotations

import datafusion_engine.delta.service as service_module
from datafusion_engine.delta.service_protocol import DeltaServicePort


def test_delta_service_factory_removed() -> None:
    assert not hasattr(service_module, "delta_service_for_profile")


def test_protocol_exports_available() -> None:
    assert hasattr(DeltaServicePort, "table_schema")
    assert hasattr(DeltaServicePort, "table_version")
