"""Tests for common macro scoring export surface."""

from __future__ import annotations

from tools.cq.macros.common import scoring


def test_common_scoring_exports_helper() -> None:
    assert callable(scoring.macro_score_payload)
