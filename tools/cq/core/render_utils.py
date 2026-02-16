"""Shared rendering utility helpers for CQ renderers."""

from __future__ import annotations

from tools.cq.core.schema import CqResult, Finding


def na(reason: str) -> str:
    """Render an explicit not-applicable reason string.

    Returns:
        str: Display string with a normalized reason suffix.
    """
    return f"N/A - {reason.replace('_', ' ')}"


def clean_scalar(value: object) -> str | None:
    """Normalize scalar values to display-safe strings.

    Returns:
        str | None: Normalized string form for supported scalar values.
    """
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return str(value)
    return None


def safe_int(value: object) -> int | None:
    """Return int values while rejecting bool and non-int inputs.

    Returns:
        int | None: Input integer when valid, otherwise ``None``.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def format_location(
    file_value: str | None,
    line_value: int | None,
    col_value: int | None,
) -> str | None:
    """Format optional file/line/col values into location text.

    Returns:
        str | None: Rendered location string when enough data is present.
    """
    if not file_value and line_value is None:
        return None
    base = file_value or "<unknown>"
    if line_value is not None and line_value > 0:
        base = f"{base}:{line_value}"
        if col_value is not None and col_value >= 0:
            base = f"{base}:{col_value}"
    return base


def extract_symbol_hint(finding: Finding) -> str | None:
    """Extract a stable symbol-like hint from finding details or message text.

    Returns:
        str | None: Best-effort symbol hint used in renderer output.
    """
    for key in ("name", "symbol", "match_text", "callee", "text"):
        value = finding.details.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().split("\n", maxsplit=1)[0]
    message = finding.message.strip()
    if not message:
        return None
    if ":" in message:
        candidate = message.rsplit(":", maxsplit=1)[1].strip()
        if candidate:
            return candidate
    if "(" in message:
        return message.split("(", maxsplit=1)[0].strip() or None
    return message


def iter_result_findings(result: CqResult) -> list[Finding]:
    """Collect all findings from a CQ result in stable render order.

    Returns:
        list[Finding]: Ordered key, section, and evidence findings.
    """
    findings: list[Finding] = []
    findings.extend(result.key_findings)
    for section in result.sections:
        findings.extend(section.findings)
    findings.extend(result.evidence)
    return findings


__all__ = [
    "clean_scalar",
    "extract_symbol_hint",
    "format_location",
    "iter_result_findings",
    "na",
    "safe_int",
]
