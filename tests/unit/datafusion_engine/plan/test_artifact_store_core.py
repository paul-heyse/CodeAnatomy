"""Tests for artifact-store core split modules."""

from __future__ import annotations

import inspect

from datafusion_engine.plan import artifact_store_cache, artifact_store_query


def test_artifact_store_split_modules_export_helpers() -> None:
    """Split artifact-store modules export expected helper functions."""
    assert callable(artifact_store_cache.ensure_plan_artifacts_table)
    assert callable(artifact_store_query.validate_plan_determinism)


def test_artifact_store_split_modules_contain_owned_logic() -> None:
    """Split artifact-store modules contain owned local implementations."""
    cache_source = inspect.getsource(artifact_store_cache)
    query_source = inspect.getsource(artifact_store_query)
    assert "def ensure_plan_artifacts_table" in cache_source
    assert "_plan_artifacts_location" in cache_source
    assert "def validate_plan_determinism" in query_source
    assert "_collect_determinism_results" in query_source
