"""Tests for plan artifact identity validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pytest

from datafusion_engine.plan.artifact_validation import validate_plan_identity
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


@dataclass
class _Bundle:
    plan_identity_hash: str | None
    plan_fingerprint: str | None


def test_validate_plan_identity_requires_hashes() -> None:
    """Validator rejects bundles missing required identity fields."""
    with pytest.raises(ValueError, match="plan_identity_hash"):
        validate_plan_identity(cast("DataFusionPlanArtifact", _Bundle(None, "fp")))
    with pytest.raises(ValueError, match="plan_fingerprint"):
        validate_plan_identity(cast("DataFusionPlanArtifact", _Bundle("id", None)))
