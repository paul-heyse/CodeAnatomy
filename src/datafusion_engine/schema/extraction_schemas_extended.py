"""Extended extraction schema facade."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.schema.extraction_schemas import (
    LIBCST_FILES_SCHEMA,
    REPO_FILES_SCHEMA,
    SCIP_METADATA_SCHEMA,
    SCIP_OCCURRENCES_SCHEMA,
)


def extended_extraction_schemas() -> dict[str, pa.Schema]:
    """Return extended extraction schemas keyed by dataset name."""
    return {
        "libcst_files_v1": LIBCST_FILES_SCHEMA,
        "repo_files_v1": REPO_FILES_SCHEMA,
        "scip_metadata_v1": SCIP_METADATA_SCHEMA,
        "scip_occurrences_v1": SCIP_OCCURRENCES_SCHEMA,
    }


__all__ = ["extended_extraction_schemas"]
