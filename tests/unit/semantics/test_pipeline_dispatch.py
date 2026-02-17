"""Tests for semantic pipeline dispatch split module."""

from __future__ import annotations

import inspect
from importlib import import_module

pipeline_dispatch = import_module("semantics.pipeline_dispatch")


def test_pipeline_dispatch_owns_dispatch_helpers() -> None:
    """Dispatch module owns local dispatch helper implementations."""
    source = inspect.getsource(pipeline_dispatch)
    assert "def _dispatch_from_registry" in source
    assert "pipeline_build as _core" not in source
