"""Tests for plan fingerprint helper bridge."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.plan import plan_fingerprint

if TYPE_CHECKING:
    from datafusion_engine.plan.plan_fingerprint import PlanFingerprintInputs


def test_compute_plan_fingerprint_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fingerprint helper should delegate to underlying hash helper."""
    monkeypatch.setattr(plan_fingerprint, "_hash_plan", lambda _inputs: "fingerprint")
    assert (
        plan_fingerprint.compute_plan_fingerprint(cast("PlanFingerprintInputs", object()))
        == "fingerprint"
    )
