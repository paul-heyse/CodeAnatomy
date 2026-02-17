"""Tests for CLI command schema projections."""

from __future__ import annotations

from tools.cq.cli_app.params import QueryParams, SearchParams
from tools.cq.cli_app.schema_projection import project_query_params, project_search_params
from tools.cq.cli_app.types import QueryLanguageToken


def test_project_search_params_maps_to_canonical_schema() -> None:
    """Search params should project into canonical command schema fields."""
    params = SearchParams(
        regex=True,
        include=["src/**"],
        exclude=["tests/**"],
        include_strings=True,
        in_dir="tools/cq",
        lang=QueryLanguageToken.rust,
    )

    schema = project_search_params(params)

    assert schema.regex is True
    assert schema.include == ["src/**"]
    assert schema.exclude == ["tests/**"]
    assert schema.include_strings is True
    assert schema.in_dir == "tools/cq"
    assert schema.lang is QueryLanguageToken.rust


def test_project_query_params_maps_to_canonical_schema() -> None:
    """Query params should project into canonical command schema fields."""
    schema = project_query_params(QueryParams(explain_files=True, include=["src/**"]))
    assert schema.explain_files is True
    assert schema.include == ["src/**"]
