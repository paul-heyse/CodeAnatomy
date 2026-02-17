"""Tests for dataset Delta registration split module."""

from __future__ import annotations

import inspect

from datafusion_engine.dataset import registration_delta


def test_registration_delta_contains_callable_helpers() -> None:
    """Registration module exposes canonical delta helper entrypoint."""
    source = inspect.getsource(registration_delta)
    assert "def _build_delta_provider_registration" in source
    assert "registration_core as _core" not in source
    assert "resolve_dataset_provider(" in source
    assert callable(registration_delta.__dict__["_build_delta_provider_registration"])
