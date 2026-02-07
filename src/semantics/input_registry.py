"""Canonical semantic input specifications."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from semantics.catalog.dataset_rows import SemanticDatasetRow


@dataclass(frozen=True)
class SemanticInputSpec:
    """Specification for a semantic pipeline input table."""

    canonical_name: str
    """Canonical name used by semantic compiler."""

    extraction_source: str
    """Name of the extraction output table."""

    required: bool = True
    """Whether this input is required for pipeline execution."""


def _semantic_input_rows() -> tuple[SemanticDatasetRow, ...]:
    from semantics.catalog.dataset_rows import get_all_dataset_rows

    return tuple(row for row in get_all_dataset_rows() if row.role == "input")


# Canonical input specifications
SEMANTIC_INPUT_SPECS: Final[tuple[SemanticInputSpec, ...]] = tuple(
    SemanticInputSpec(
        canonical_name=row.name,
        extraction_source=row.source_dataset or row.name,
        required=True,
    )
    for row in _semantic_input_rows()
)


__all__ = [
    "SEMANTIC_INPUT_SPECS",
    "SemanticInputSpec",
]
