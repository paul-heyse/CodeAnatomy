"""Schema metadata specification helpers.

This module provides the schema-spec authority surface for metadata extraction
without duplicating implementation logic.
"""

from __future__ import annotations

from datafusion_engine.arrow.metadata import (
    SchemaMetadataSpec,
    metadata_spec_from_schema,
)

__all__ = ["SchemaMetadataSpec", "metadata_spec_from_schema"]
