# ruff: noqa: D100, D103, PT011, B903
from __future__ import annotations

from typing import cast

import pytest

from datafusion_engine.plan.artifact_validation import validate_plan_identity
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


class _Bundle:
    def __init__(self, plan_identity_hash: str | None, plan_fingerprint: str | None) -> None:
        self.plan_identity_hash = plan_identity_hash
        self.plan_fingerprint = plan_fingerprint


def test_validate_plan_identity_requires_hashes() -> None:
    with pytest.raises(ValueError):
        validate_plan_identity(cast("DataFusionPlanArtifact", _Bundle(None, "fp")))
    with pytest.raises(ValueError):
        validate_plan_identity(cast("DataFusionPlanArtifact", _Bundle("id", None)))
