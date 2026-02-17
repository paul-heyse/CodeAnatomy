"""Tests for plan artifact persistence helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from datafusion_engine.plan.artifact_persistence import persist_bundle
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


@dataclass
class _Row:
    plan_identity_hash: str


@dataclass
class _Store:
    @staticmethod
    def persist_plan(bundle: DataFusionPlanArtifact) -> object:
        _ = bundle
        return _Row(plan_identity_hash="abc")


def test_persist_bundle_returns_identity_hash() -> None:
    """Persist helper returns identity hash from row payload."""
    assert persist_bundle(_Store(), cast("DataFusionPlanArtifact", object())) == "abc"
