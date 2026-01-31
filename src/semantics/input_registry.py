"""Input registry for semantic pipeline - maps extraction outputs to semantic inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from datafusion import SessionContext


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


# Canonical input specifications
SEMANTIC_INPUT_SPECS: Final[tuple[SemanticInputSpec, ...]] = (
    SemanticInputSpec(
        canonical_name="cst_refs",
        extraction_source="cst_refs",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_defs",
        extraction_source="cst_defs",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_imports",
        extraction_source="cst_imports",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_callsites",
        extraction_source="cst_callsites",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_call_args",
        extraction_source="cst_call_args",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_docstrings",
        extraction_source="cst_docstrings",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_decorators",
        extraction_source="cst_decorators",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="scip_occurrences",
        extraction_source="scip_occurrences",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="file_line_index_v1",
        extraction_source="file_line_index_v1",
        required=True,  # Required for SCIP byte offset conversion
    ),
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
    from datafusion_engine.schema.introspection import table_names_snapshot

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
