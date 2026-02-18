"""Tests for summary variant contracts."""

from __future__ import annotations

from tools.cq.core.schema import mk_result, mk_runmeta
from tools.cq.core.summary_types import (
    CallsSummaryV1,
    ImpactSummaryV1,
    NeighborhoodSummaryV1,
    RunSummaryV1,
    SearchSummaryV1,
    SummaryEnvelopeV1,
    resolve_summary_variant_name,
    summary_for_macro,
    summary_from_mapping,
)


def _run(macro: str) -> tuple[str, SummaryEnvelopeV1]:
    run = mk_runmeta(
        macro=macro,
        argv=[],
        root=".",
        started_ms=0.0,
        toolchain={},
        run_id="run-1",
    )
    return macro, mk_result(run).summary


def test_summary_for_macro_selects_expected_variant() -> None:
    """Macro-to-variant mapping uses the canonical summary variants."""
    assert isinstance(summary_for_macro("q"), SearchSummaryV1)
    assert isinstance(summary_for_macro("calls"), CallsSummaryV1)
    assert isinstance(summary_for_macro("impact"), ImpactSummaryV1)
    assert isinstance(summary_for_macro("run"), RunSummaryV1)
    assert isinstance(summary_for_macro("neighborhood"), NeighborhoodSummaryV1)


def test_summary_from_mapping_selects_variant_from_mode() -> None:
    """Mode values resolve to expected summary variants."""
    assert isinstance(summary_from_mapping({"mode": "entity"}), SearchSummaryV1)
    assert isinstance(summary_from_mapping({"mode": "macro:calls"}), CallsSummaryV1)
    assert isinstance(summary_from_mapping({"mode": "sig-impact"}), ImpactSummaryV1)
    assert isinstance(summary_from_mapping({"mode": "run"}), RunSummaryV1)
    assert isinstance(summary_from_mapping({"mode": "neighborhood"}), NeighborhoodSummaryV1)


def test_summary_from_mapping_respects_explicit_summary_variant() -> None:
    """Explicit summary_variant tag takes precedence over mode inference."""
    summary = summary_from_mapping({"mode": "entity", "summary_variant": "run"})
    assert isinstance(summary, RunSummaryV1)
    assert summary.summary_variant == "run"


def test_mk_result_uses_macro_typed_summary_variant() -> None:
    """Result construction assigns summary variant based on macro."""
    macro, summary = _run("calls")
    assert macro == "calls"
    assert isinstance(summary, CallsSummaryV1)


def test_resolve_summary_variant_name_defaults_to_search() -> None:
    """Unknown mode/macro falls back to the search summary envelope."""
    assert resolve_summary_variant_name(mode="unknown", macro="unknown") == "search"
