"""Integration tests for DataFusion UDF registry snapshots."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_registry import TREE_SITTER_CHECK_VIEWS

_CACHE_TABLES: tuple[str, ...] = (
    "list_files_cache",
    "metadata_cache",
    "statistics_cache",
    "predicate_cache",
)
from obs.diagnostics import DiagnosticsCollector

if TYPE_CHECKING:
    from datafusion import SessionContext

pytest.importorskip("datafusion")


@pytest.mark.integration
def test_udf_registry_snapshot_includes_capsule_payload() -> None:
    """Record UDF registry payloads while executing a UDF query."""
    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(
        diagnostics_sink=sink,
        input_plugins=(_seed_tree_sitter_check_views,),
    )
    ctx = profile.session_context()
    df = ctx.sql("SELECT stable_hash64('alpha') AS hash_value")
    table = df.to_arrow_table()
    assert table.num_rows == 1
    artifacts = sink.artifacts_snapshot().get("datafusion_udf_registry_v1", [])
    assert artifacts
    payload = artifacts[-1]
    scalar_names = payload.get("scalar", [])
    assert isinstance(scalar_names, list)
    scalar_strings = [name for name in scalar_names if isinstance(name, str)]
    assert "stable_hash64" in scalar_strings
    assert "pycapsule_udfs" in payload


def _seed_tree_sitter_check_views(ctx: SessionContext) -> None:
    """Seed diagnostics tables required during runtime initialization."""
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    table = pa.table({"mismatch": pa.array([], type=pa.bool_())})
    for name in TREE_SITTER_CHECK_VIEWS:
        if ctx.table_exist(name):
            continue
        adapter.register_arrow_table(name, table)
    cache_table = pa.table(
        {
            "cache_name": pa.array([], type=pa.string()),
            "event_time_unix_ms": pa.array([], type=pa.int64()),
            "entry_count": pa.array([], type=pa.int64()),
            "hit_count": pa.array([], type=pa.int64()),
            "miss_count": pa.array([], type=pa.int64()),
            "eviction_count": pa.array([], type=pa.int64()),
            "config_ttl": pa.array([], type=pa.string()),
            "config_limit": pa.array([], type=pa.string()),
        }
    )
    for name in _CACHE_TABLES:
        if ctx.table_exist(name):
            continue
        adapter.register_arrow_table(name, cache_table)
