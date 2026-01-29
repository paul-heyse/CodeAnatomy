"""Unit tests for Delta write policy helpers."""

from __future__ import annotations

import sys

import pytest

pytest.importorskip("datafusion")
pytest.importorskip("deltalake")

from test_support import datafusion_ext_stub

sys.modules.setdefault("datafusion_ext", datafusion_ext_stub)

from datafusion_engine.write_pipeline import _delta_schema_mode, _writer_properties_from_policy
from storage.deltalake.config import DeltaSchemaPolicy, ParquetWriterPolicy


def test_delta_schema_mode_prefers_options_over_policy() -> None:
    """Prefer explicit schema_mode over policy defaults."""
    policy = DeltaSchemaPolicy(schema_mode="merge")
    value = _delta_schema_mode({"schema_mode": "overwrite"}, schema_policy=policy)
    assert value == "overwrite"


def test_delta_schema_mode_uses_policy_when_unset() -> None:
    """Use schema_mode from the policy when options are empty."""
    policy = DeltaSchemaPolicy(schema_mode="merge")
    value = _delta_schema_mode({}, schema_policy=policy)
    assert value == "merge"


def test_writer_properties_from_policy_sets_column_options() -> None:
    """Build writer properties with bloom/dict/stats settings."""
    policy = ParquetWriterPolicy(
        statistics_enabled=("node_id",),
        bloom_filter_enabled=("node_id", "path"),
        dictionary_enabled=("path",),
        bloom_filter_fpp=0.01,
        bloom_filter_ndv=100,
    )
    writer_props = _writer_properties_from_policy(policy)
    assert writer_props is not None
    column_props = writer_props.column_properties
    assert column_props is not None
    assert set(column_props) == {"node_id", "path"}
    node_props = column_props["node_id"]
    assert node_props.statistics_enabled == "PAGE"
    assert node_props.dictionary_enabled is None
    assert node_props.bloom_filter_properties is not None
    path_props = column_props["path"]
    assert path_props.dictionary_enabled is True
    assert path_props.statistics_enabled is None
    assert path_props.bloom_filter_properties is not None
