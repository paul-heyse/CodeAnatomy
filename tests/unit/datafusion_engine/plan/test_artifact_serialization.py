"""Tests for plan artifact serialization payload helpers."""

from __future__ import annotations

from typing import cast

from datafusion_engine.plan.artifact_serialization import bundle_payload
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


class _Bundle:
    plan_identity_hash = "id"
    plan_fingerprint = "fp"
    substrait_bytes = b"bytes"
    execution_plan = object()


def test_bundle_payload_exposes_identity_fields() -> None:
    """Serialized payload exposes plan identity and fingerprint fields."""
    payload = bundle_payload(cast("DataFusionPlanArtifact", _Bundle()))
    assert payload["plan_identity_hash"] == "id"
    assert payload["plan_fingerprint"] == "fp"
    assert payload["has_execution_plan"] is True
