"""Input registry for semantic pipeline - maps extraction outputs to semantic inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from datafusion_engine.extract.bundles import dataset_name_for_output

if TYPE_CHECKING:
    from datafusion import SessionContext

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

    fallback_names: tuple[str, ...] = ()
    """Alternative table names to try if primary is missing."""


def _fallback_names(output: str) -> tuple[str, ...]:
    candidates: list[str] = []
    try:
        dataset = dataset_name_for_output(output)
    except KeyError:
        dataset = None
    candidates.append(output)
    if dataset is not None and dataset != output:
        candidates.append(dataset)
    if output.endswith("_v1"):
        candidates.append(output.removesuffix("_v1"))
        return tuple(dict.fromkeys(candidates))
    versioned = f"{output}_v1"
    candidates.append(versioned)
    try:
        dataset_versioned = dataset_name_for_output(versioned)
    except KeyError:
        dataset_versioned = None
    if dataset_versioned is not None and dataset_versioned not in candidates:
        candidates.append(dataset_versioned)
    return tuple(dict.fromkeys(candidates))


def _semantic_input_rows() -> tuple[SemanticDatasetRow, ...]:
    from semantics.catalog.dataset_rows import get_all_dataset_rows

    return tuple(row for row in get_all_dataset_rows() if row.role == "input")


# Canonical input specifications
SEMANTIC_INPUT_SPECS: Final[tuple[SemanticInputSpec, ...]] = tuple(
    SemanticInputSpec(
        canonical_name=row.name,
        extraction_source=row.source_dataset or row.name,
        required=True,
        fallback_names=_fallback_names(row.source_dataset or row.name),
    )
    for row in _semantic_input_rows()
)


@dataclass(frozen=True)
class InputValidationResult:
    """Result of validating semantic pipeline inputs."""

    valid: bool
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]
    resolved_names: dict[str, str]


def validate_semantic_inputs(ctx: SessionContext) -> InputValidationResult:
    """Validate that all required semantic inputs are available.

    Parameters
    ----------
    ctx
        DataFusion session context with registered tables.

    Returns
    -------
    InputValidationResult
        Validation result with missing tables and resolved names.
    """
    from datafusion_engine.catalog.introspection import invalidate_introspection_cache
    from datafusion_engine.schema.introspection import table_names_snapshot

    invalidate_introspection_cache(ctx)
    available = set(table_names_snapshot(ctx))
    missing_required: list[str] = []
    missing_optional: list[str] = []
    resolved: dict[str, str] = {}

    for spec in SEMANTIC_INPUT_SPECS:
        # Try primary name first
        if spec.extraction_source in available:
            resolved[spec.canonical_name] = spec.extraction_source
            continue

        # Try fallback names
        found = False
        for fallback in spec.fallback_names:
            if fallback in available:
                resolved[spec.canonical_name] = fallback
                found = True
                break

        if not found:
            if spec.required:
                missing_required.append(spec.canonical_name)
            else:
                missing_optional.append(spec.canonical_name)

    return InputValidationResult(
        valid=len(missing_required) == 0,
        missing_required=tuple(missing_required),
        missing_optional=tuple(missing_optional),
        resolved_names=resolved,
    )


def require_semantic_inputs(ctx: SessionContext) -> dict[str, str]:
    """Validate inputs and return resolved name mapping.

    Returns
    -------
    dict[str, str]
        Mapping from canonical names to resolved table names.

    Raises
    ------
    ValueError
        If required inputs are missing.
    """
    result = validate_semantic_inputs(ctx)
    if not result.valid:
        msg = f"Missing required semantic inputs: {result.missing_required}"
        raise ValueError(msg)
    return result.resolved_names


__all__ = [
    "SEMANTIC_INPUT_SPECS",
    "InputValidationResult",
    "SemanticInputSpec",
    "require_semantic_inputs",
    "validate_semantic_inputs",
]
