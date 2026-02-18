"""Tests for plan-module public export surfaces."""

from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "datafusion_engine.plan.bundle_assembly",
        "datafusion_engine.plan.plan_proto",
        "datafusion_engine.plan.artifact_store_constants",
        "datafusion_engine.plan.artifact_store_persistence",
    ],
)
def test_plan_module_all_excludes_private_names(module_name: str) -> None:
    """Plan module __all__ surfaces expose only public names."""
    module = importlib.import_module(module_name)
    export_names = [str(name) for name in getattr(module, "__all__", ())]
    assert [name for name in export_names if name.startswith("_")] == []
