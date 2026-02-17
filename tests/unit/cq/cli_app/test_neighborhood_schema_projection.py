"""Neighborhood params/schema projection tests."""

from __future__ import annotations

from tools.cq.cli_app.params import NeighborhoodParams
from tools.cq.cli_app.schema_projection import (
    neighborhood_options_from_projected_params,
    project_neighborhood_params,
)
from tools.cq.cli_app.types import NeighborhoodLanguageToken

DEFAULT_TOP_K = 10
EXPLICIT_TOP_K = 7


def test_project_neighborhood_params_defaults() -> None:
    """Projection should materialize canonical defaults."""
    schema = project_neighborhood_params(None)
    assert schema.lang is NeighborhoodLanguageToken.python
    assert schema.top_k == DEFAULT_TOP_K
    assert schema.semantic_enrichment is True
    assert schema.enrich is True
    assert schema.enrich_mode == "ts_sym"


def test_project_neighborhood_params_from_explicit_values() -> None:
    """Projection should preserve explicit option values."""
    schema = project_neighborhood_params(
        NeighborhoodParams(
            lang=NeighborhoodLanguageToken.rust,
            top_k=EXPLICIT_TOP_K,
            semantic_enrichment=False,
            enrich=False,
            enrich_mode="full",
        )
    )
    assert schema.lang is NeighborhoodLanguageToken.rust
    assert schema.top_k == EXPLICIT_TOP_K
    assert schema.semantic_enrichment is False
    assert schema.enrich is False
    assert schema.enrich_mode == "full"


def test_neighborhood_options_projection_matches_schema_projection() -> None:
    """Options projection should match schema projection field-for-field."""
    params = NeighborhoodParams(
        lang=NeighborhoodLanguageToken.rust,
        top_k=3,
        semantic_enrichment=False,
        enrich=True,
        enrich_mode="ts_sym_dis",
    )
    schema = project_neighborhood_params(params)
    options = neighborhood_options_from_projected_params(params)
    assert options == schema
