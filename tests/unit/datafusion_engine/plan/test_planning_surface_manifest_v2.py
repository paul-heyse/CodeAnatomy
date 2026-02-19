"""Planning surface manifest v2 parity tests."""

from __future__ import annotations

from datafusion_engine.plan.contracts import PlanningSurfaceManifestV2
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_extensions import planning_surface_manifest_v2_payload


def test_planning_surface_manifest_v2_contract_defaults() -> None:
    """Manifest contract defaults expose empty planner/factory collections."""
    manifest = PlanningSurfaceManifestV2()
    assert manifest.expr_planners == ()
    assert manifest.relation_planners == ()
    assert manifest.type_planners == ()
    assert manifest.table_factories == ()
    assert manifest.planning_config_keys == {}


def test_runtime_manifest_payload_includes_v2_fields() -> None:
    """Runtime payload exports the full v2 manifest surface."""
    profile = DataFusionRuntimeProfile()
    payload = planning_surface_manifest_v2_payload(profile)
    assert "expr_planners" in payload
    assert "relation_planners" in payload
    assert "type_planners" in payload
    assert "table_factories" in payload
    assert "planning_config_keys" in payload
