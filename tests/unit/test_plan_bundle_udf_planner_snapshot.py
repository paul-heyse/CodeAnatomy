"""Tests for UDF planner snapshot capture in plan bundles."""

from __future__ import annotations

from dataclasses import replace

import pyarrow as pa
import pytest

from datafusion_engine.domain_planner import domain_planner_names_from_snapshot
from datafusion_engine.plan_bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.udf_catalog import rewrite_tag_index
from datafusion_engine.udf_runtime import rust_udf_snapshot_hash

datafusion = pytest.importorskip("datafusion")


def _udf_snapshot_payload() -> dict[str, object]:
    return {
        "scalar": ["my_udf"],
        "aggregate": [],
        "window": [],
        "table": [],
        "aliases": {},
        "parameter_names": {"my_udf": ["x", "y"]},
        "volatility": {"my_udf": "immutable"},
        "rewrite_tags": {"my_udf": ["tag_a"]},
        "signature_inputs": {"my_udf": [["Int64", "Utf8"]]},
        "return_types": {"my_udf": ["Utf8"]},
        "simplify": {"my_udf": True},
        "coerce_types": {"my_udf": False},
        "short_circuits": {"my_udf": True},
        "custom_udfs": [],
    }


def test_plan_bundle_captures_udf_planner_snapshot() -> None:
    """Ensure planner metadata is recorded from UDF snapshots."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    snapshot = _udf_snapshot_payload()
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    rewrite_tags = tuple(sorted(rewrite_tag_index(snapshot)))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    session_runtime = replace(
        session_runtime,
        udf_snapshot=snapshot,
        udf_snapshot_hash=snapshot_hash,
        udf_rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
    )
    ctx.register_record_batches(
        "events",
        [pa.table({"id": [1, 2], "label": ["a", "b"]}).to_batches()],
    )
    df = ctx.sql("SELECT id FROM events")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    planner_snapshot = bundle.artifacts.udf_planner_snapshot
    assert isinstance(planner_snapshot, dict)
    assert planner_snapshot.get("status") == "ok"
    functions = planner_snapshot.get("functions")
    assert isinstance(functions, list)
    function = next(item for item in functions if item.get("name") == "my_udf")
    assert function.get("volatility") == "immutable"
    assert function.get("parameter_names") == ("x", "y")
    signature = function.get("signature")
    assert isinstance(signature, dict)
    assert signature.get("inputs") == (("Int64", "Utf8"),)
    assert signature.get("returns") == ("Utf8",)
    assert function.get("return_type") == "Utf8"
    assert function.get("rewrite_tags") == ("tag_a",)
    assert function.get("has_simplify") is True
    assert function.get("has_coerce_types") is False
    assert function.get("short_circuits") is True
