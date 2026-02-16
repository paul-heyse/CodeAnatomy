"""Write helpers for Arrow schema metadata."""

from __future__ import annotations

from datafusion_engine.arrow.metadata import SchemaMetadataSpec, merge_metadata_specs


def merge_schema_metadata(*specs: SchemaMetadataSpec | None) -> SchemaMetadataSpec:
    """Merge schema metadata specifications.

    Returns:
    -------
    SchemaMetadataSpec
        Combined metadata specification.
    """
    return merge_metadata_specs(*specs)


__all__ = ["merge_schema_metadata"]
