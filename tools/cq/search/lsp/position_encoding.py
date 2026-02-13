"""LSP position-encoding conversion helpers."""

from __future__ import annotations

from collections.abc import Callable

_UTF_8 = "utf-8"
_UTF_16 = "utf-16"
_UTF_32 = "utf-32"


def to_lsp_character(line_text: str, column: int, encoding: str) -> int:
    """Convert CQ character-column index to LSP ``Position.character`` units.

    Returns:
        Encoded LSP character offset for the provided line/column.
    """
    normalized_col = max(0, int(column))
    if normalized_col <= 0:
        return 0
    prefix = line_text[:normalized_col]
    if encoding == _UTF_8:
        return len(prefix.encode("utf-8"))
    if encoding == _UTF_16:
        return len(prefix.encode("utf-16-le")) // 2
    if encoding == _UTF_32:
        return len(prefix.encode("utf-32-le")) // 4
    return min(normalized_col, len(line_text))


def from_lsp_character(line_text: str, character: int, encoding: str) -> int:
    """Convert LSP ``Position.character`` units to CQ character-column index.

    Returns:
        Decoded CQ character-column index.
    """
    target = max(0, int(character))
    if target <= 0:
        return 0
    if encoding not in {_UTF_8, _UTF_16, _UTF_32}:
        return min(target, len(line_text))

    unit_width = _unit_width_for_encoding(encoding)
    return _units_to_codepoint_index(
        line_text=line_text,
        target_units=target,
        unit_count_fn=lambda ch: len(ch.encode(_encoding_name(encoding))) // unit_width,
    )


def _units_to_codepoint_index(
    *,
    line_text: str,
    target_units: int,
    unit_count_fn: Callable[[str], int],
) -> int:
    units = 0
    for idx, ch in enumerate(line_text):
        next_units = units + unit_count_fn(ch)
        if target_units < next_units:
            return idx
        if target_units == next_units:
            return idx + 1
        units = next_units
    return len(line_text)


def _unit_width_for_encoding(encoding: str) -> int:
    if encoding == _UTF_16:
        return 2
    if encoding == _UTF_32:
        return 4
    return 1


def _encoding_name(encoding: str) -> str:
    if encoding == _UTF_16:
        return "utf-16-le"
    if encoding == _UTF_32:
        return "utf-32-le"
    return "utf-8"


__all__ = ["from_lsp_character", "to_lsp_character"]
