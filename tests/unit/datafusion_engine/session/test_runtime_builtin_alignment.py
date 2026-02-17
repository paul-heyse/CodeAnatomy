# ruff: noqa: D100, D103
from __future__ import annotations

from datafusion_engine.catalog import introspection
from datafusion_engine.session.runtime_config_policies import DEFAULT_DF_POLICY


def test_default_runtime_policy_contains_builtin_datafusion_tuning_keys() -> None:
    settings = DEFAULT_DF_POLICY.settings
    assert settings["datafusion.execution.collect_statistics"] == "true"
    assert settings["datafusion.execution.planning_concurrency"] == "8"
    assert settings["datafusion.execution.parquet.max_predicate_cache_size"]
    assert settings["datafusion.runtime.metadata_cache_limit"]


def test_catalog_introspection_exports_builtin_cache_snapshots() -> None:
    assert callable(introspection.list_files_cache_snapshot)
    assert callable(introspection.statistics_cache_snapshot)
    assert callable(introspection.predicate_cache_snapshot)
