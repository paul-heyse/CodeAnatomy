"""Helpers for object-level fact applicability and rendering policy."""

from __future__ import annotations


def is_applicability_not_applicable(reason: str | None) -> bool:
    """Return whether an applicability reason indicates non-applicability.

    Returns:
    -------
    bool
        ``True`` when reason denotes a not-applicable state.
    """
    if not isinstance(reason, str):
        return False
    return reason.startswith("not_applicable")


def suppress_not_applicable_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Drop not-applicable rows from terminal-facing payload tables.

    Returns:
    -------
    list[dict[str, object]]
        Rows that should remain visible in terminal output.
    """
    out: list[dict[str, object]] = []
    for row in rows:
        reason = row.get("reason") if isinstance(row, dict) else None
        if isinstance(reason, str) and is_applicability_not_applicable(reason):
            continue
        out.append(row)
    return out


__all__ = [
    "is_applicability_not_applicable",
    "suppress_not_applicable_rows",
]
