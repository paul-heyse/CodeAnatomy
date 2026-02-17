"""Tests for options construction from projected command schemas."""

from __future__ import annotations

from tools.cq.cli_app.params import QueryParams, RunParams, SearchParams
from tools.cq.cli_app.schema_projection import (
    query_options_from_projected_params,
    run_options_from_projected_params,
    search_options_from_projected_params,
)


def test_search_options_from_projected_params_preserves_flags() -> None:
    """Projected search options should preserve mode and enrichment flags."""
    options = search_options_from_projected_params(
        SearchParams(regex=True, enrich=False, enrich_mode="ts_only"),
    )

    assert options.regex is True
    assert options.enrich is False
    assert options.enrich_mode == "ts_only"


def test_query_options_from_projected_params_preserves_explain_flag() -> None:
    """Projected query options should preserve explain-files behavior."""
    options = query_options_from_projected_params(QueryParams(explain_files=True))
    assert options.explain_files is True


def test_run_options_from_projected_params_preserves_stop_on_error() -> None:
    """Projected run options should preserve stop-on-error behavior."""
    options = run_options_from_projected_params(RunParams(stop_on_error=True))
    assert options.stop_on_error is True
