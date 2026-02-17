"""Unit tests for relspec metadata helper surface."""

from __future__ import annotations

from relspec.contracts import RELATION_OUTPUT_NAME
from relspec.metadata import relation_output_metadata_spec, relspec_metadata_spec


def test_relation_output_metadata_spec() -> None:
    """Relation output metadata spec exposes schema metadata."""
    spec = relation_output_metadata_spec()
    assert spec.schema_metadata


def test_relspec_metadata_spec_for_relation_output() -> None:
    """Metadata lookup by relation output name returns schema metadata."""
    spec = relspec_metadata_spec(RELATION_OUTPUT_NAME)
    assert spec.schema_metadata
