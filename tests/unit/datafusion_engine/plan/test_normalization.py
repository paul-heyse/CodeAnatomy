"""Tests for Substrait plan normalization strictness."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.plan import normalization

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan


def test_normalize_substrait_plan_returns_normalized_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normalization should return wrapper-stripped plans when supported."""
    sentinel_plan = object()
    monkeypatch.setattr(normalization, "_strip_substrait_wrappers", lambda _plan: sentinel_plan)
    monkeypatch.setattr(normalization, "_unsupported_substrait_variants", lambda _plan: set())

    assert (
        normalization.normalize_substrait_plan(
            cast("SessionContext", object()),
            cast("DataFusionLogicalPlan", object()),
        )
        is sentinel_plan
    )


def test_normalize_substrait_plan_raises_on_unsupported_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normalization should fail fast when unsupported variants remain."""
    sentinel_plan = object()
    monkeypatch.setattr(normalization, "_strip_substrait_wrappers", lambda _plan: sentinel_plan)
    monkeypatch.setattr(
        normalization,
        "_unsupported_substrait_variants",
        lambda _plan: {"Explain", "Analyze"},
    )

    with pytest.raises(ValueError, match="unsupported logical-plan variants"):
        normalization.normalize_substrait_plan(
            cast("SessionContext", object()),
            cast("DataFusionLogicalPlan", object()),
        )
