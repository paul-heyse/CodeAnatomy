"""Summary envelope validation helpers."""

from __future__ import annotations

from tools.cq.core.summary_types import SummaryEnvelopeV1

__all__ = ["validate_summary_envelope", "validate_summary_sections"]


def validate_summary_envelope(envelope: SummaryEnvelopeV1) -> list[str]:
    """Validate high-level summary envelope shape.

    Returns:
        list[str]: Validation errors, empty when valid.
    """
    errors: list[str] = []
    if not isinstance(envelope.query, (str, type(None))):
        errors.append("query must be a string or None")
    if not isinstance(envelope.mode, (str, type(None))):
        errors.append("mode must be a string or None")
    if not hasattr(envelope, "__struct_fields__"):
        errors.append("summary struct inspection failed: missing __struct_fields__")
    return errors


def validate_summary_sections(sections: tuple[object, ...]) -> list[str]:
    """Validate summary section container.

    Returns:
        list[str]: Validation errors for section payload shape.
    """
    errors: list[str] = []
    if not isinstance(sections, tuple):
        errors.append("sections must be a tuple")
    for index, section in enumerate(sections):
        if not isinstance(section, str):
            errors.append(f"section[{index}] must be a string")
    return errors
