"""Unit tests for DataFusion unparser plan artifacts."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.ingest import datafusion_from_arrow
from datafusion_engine.plan_bundle import PlanBundleOptions, build_plan_bundle
from datafusion_engine.runtime import DataFusionRuntimeProfile

datafusion = pytest.importorskip("datafusion")


def test_datafusion_unparser_payload_is_deterministic() -> None:
    """Capture deterministic plan display payloads for bundles."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    datafusion_from_arrow(
        ctx,
        name="events",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    df = ctx.sql("SELECT id, label FROM events")
    first = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    second = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    assert first.display_optimized_plan() == second.display_optimized_plan()


def test_plan_bundle_includes_fingerprint() -> None:
    """Record plan fingerprints for DataFusion plan bundles."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    datafusion_from_arrow(
        ctx,
        name="events",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    df = ctx.sql("SELECT events.id FROM events WHERE events.id = 1")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    assert bundle.plan_fingerprint


def test_plan_bundle_projection_is_deterministic() -> None:
    """Record projection requirements for DataFusion plan bundles."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    datafusion_from_arrow(
        ctx,
        name="events",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    df = ctx.sql("SELECT events.id FROM events WHERE events.id = 1")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    plan_text = bundle.display_optimized_plan() or bundle.display_logical_plan() or ""
    assert "label" not in plan_text


def test_plan_bundle_graphviz_is_optional() -> None:
    """Capture optional GraphViz payloads for plan bundles."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    datafusion_from_arrow(
        ctx,
        name="events",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    df = ctx.sql("SELECT id FROM events")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    _ = bundle.graphviz()
