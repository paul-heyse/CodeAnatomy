"""Tests for extraction plan-artifact envelope identity metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.plan import bundle_assembly
from datafusion_engine.plan.plan_identity import PLAN_IDENTITY_PAYLOAD_VERSION

if TYPE_CHECKING:
    from datafusion import SessionContext


def test_extraction_plan_artifact_envelope_contains_identity_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Envelope payload should include stable planning identity fields."""
    monkeypatch.setattr(
        bundle_assembly,
        "_df_settings_snapshot",
        lambda _ctx, **_kwargs: {"a": "1", "b": "2"},
    )
    monkeypatch.setattr(
        bundle_assembly,
        "_information_schema_snapshot",
        lambda _ctx, **_kwargs: {"tables": ["t1"]},
    )
    monkeypatch.setattr(
        bundle_assembly,
        "_information_schema_hash",
        lambda _snapshot: "info-hash",
    )
    monkeypatch.setattr(
        bundle_assembly,
        "planning_env_snapshot",
        lambda _session_runtime: {
            "planning_surface_policy_version": "v1",
            "planning_surface_policy_hash": "policy-hash",
        },
    )
    monkeypatch.setattr(
        bundle_assembly,
        "planning_env_hash",
        lambda _snapshot: "env-hash",
    )

    envelope = bundle_assembly.extraction_plan_artifact_envelope(
        cast("SessionContext", object()),
        stage="ast",
    )

    assert envelope["stage"] == "ast"
    assert envelope["information_schema_hash"] == "info-hash"
    assert envelope["planning_env_hash"] == "env-hash"
    assert envelope["planning_surface_policy_version"] == "v1"
    assert envelope["planning_surface_policy_hash"] == "policy-hash"
    assert envelope["plan_identity_payload_version"] == PLAN_IDENTITY_PAYLOAD_VERSION
    assert isinstance(envelope["identity_hash"], str)
    assert envelope["plan_identity_hash"] == envelope["identity_hash"]
