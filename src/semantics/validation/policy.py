"""Unified semantic-input validation policies."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from datafusion_engine.schema import validate_semantic_types
from semantics.validation.catalog_validation import (
    SemanticInputValidationResult,
    validate_semantic_input_columns,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from semantics.program_manifest import SemanticProgramManifest


SemanticInputValidationPolicy = Literal[
    "schema_only",
    "schema_plus_optional_probe",
    "schema_plus_runtime_probe",
]

_SEMANTIC_TYPE_VALIDATION_TABLE = "__semantic_types__"


class SemanticInputValidationError(ValueError):
    """Raised when semantic input validation fails."""

    def __init__(
        self,
        validation: SemanticInputValidationResult,
        *,
        semantic_type_issue: str | None = None,
    ) -> None:
        """__init__."""
        self.validation = validation
        missing_tables = ", ".join(sorted(validation.missing_tables))
        missing_columns = {
            table: sorted(columns) for table, columns in validation.missing_columns.items()
        }
        msg = (
            "Semantic input validation failed. "
            f"Missing tables: {missing_tables or 'none'}. "
            f"Missing columns: {missing_columns or 'none'}."
        )
        if semantic_type_issue is not None:
            msg += f" Semantic type issue: {semantic_type_issue}"
        super().__init__(msg)


@dataclass(frozen=True)
class _PresenceValidationResult:
    valid: bool
    missing_required: tuple[str, ...]
    resolved_names: dict[str, str]


def _validate_semantic_input_presence(ctx: SessionContext) -> _PresenceValidationResult:
    from datafusion_engine.catalog.introspection import invalidate_introspection_cache
    from datafusion_engine.schema.introspection import table_names_snapshot
    from semantics.input_registry import SEMANTIC_INPUT_SPECS

    invalidate_introspection_cache(ctx)
    available = set(table_names_snapshot(ctx))
    missing_required: list[str] = []
    resolved: dict[str, str] = {}

    for spec in SEMANTIC_INPUT_SPECS:
        if spec.extraction_source in available:
            resolved[spec.canonical_name] = spec.extraction_source
            continue
        if spec.required:
            missing_required.append(spec.canonical_name)

    return _PresenceValidationResult(
        valid=len(missing_required) == 0,
        missing_required=tuple(missing_required),
        resolved_names=resolved,
    )


def resolve_semantic_input_mapping(ctx: SessionContext) -> dict[str, str]:
    """Resolve and validate required semantic input table mapping.

    Returns:
        dict[str, str]: Mapping from canonical semantic dataset names to resolved table names.

    Raises:
        ValueError: If any required semantic inputs are missing.
    """
    presence = _validate_semantic_input_presence(ctx)
    if not presence.valid:
        msg = f"Missing required semantic inputs: {presence.missing_required}"
        raise ValueError(msg)
    return dict(presence.resolved_names)


def validate_semantic_inputs(
    *,
    ctx: SessionContext,
    manifest: SemanticProgramManifest,
    policy: SemanticInputValidationPolicy,
) -> SemanticInputValidationResult:
    """Validate semantic inputs using the unified policy engine.

    Returns:
    -------
    SemanticInputValidationResult
        Validation result covering required tables/columns and policy probes.
    """
    mapping = dict(manifest.input_mapping)
    validation = validate_semantic_input_columns(ctx, input_mapping=mapping)
    if not validation.valid:
        return validation
    if policy == "schema_only":
        return validation
    allow_row_probe_fallback = policy == "schema_plus_runtime_probe"
    try:
        validate_semantic_types(
            ctx,
            allow_row_probe_fallback=allow_row_probe_fallback,
        )
    except (RuntimeError, TypeError, ValueError):
        missing = (*validation.missing_tables, _SEMANTIC_TYPE_VALIDATION_TABLE)
        return replace(validation, valid=False, missing_tables=missing)
    return validation


__all__ = [
    "SemanticInputValidationError",
    "SemanticInputValidationPolicy",
    "resolve_semantic_input_mapping",
    "validate_semantic_inputs",
]
