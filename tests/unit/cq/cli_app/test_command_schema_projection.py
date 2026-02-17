"""Tests for CLI command schema projections."""

from __future__ import annotations

from tools.cq.cli_app.params import NeighborhoodParams, QueryParams, SearchParams
from tools.cq.cli_app.schema_projection import (
    project_neighborhood_params,
    project_query_params,
    project_search_params,
)
from tools.cq.cli_app.types import NeighborhoodLanguageToken, QueryLanguageToken

EXPECTED_NEIGHBORHOOD_TOP_K = 25


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


def test_project_neighborhood_params_maps_to_canonical_schema() -> None:
    """Neighborhood params should project into canonical command schema fields."""
    schema = project_neighborhood_params(
        NeighborhoodParams(
            lang=NeighborhoodLanguageToken.rust,
            top_k=EXPECTED_NEIGHBORHOOD_TOP_K,
            semantic_enrichment=False,
            enrich=False,
            enrich_mode="ts_only",
        )
    )
    assert schema.lang is NeighborhoodLanguageToken.rust
    assert schema.top_k == EXPECTED_NEIGHBORHOOD_TOP_K
    assert schema.semantic_enrichment is False
    assert schema.enrich is False
    assert schema.enrich_mode == "ts_only"
