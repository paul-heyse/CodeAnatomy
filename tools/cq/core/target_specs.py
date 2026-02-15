"""Unified target spec parsing for CQ bundles and neighborhood commands."""

from __future__ import annotations

from typing import Literal

from tools.cq.core.structs import CqStruct

BundleTargetKind = Literal["function", "class", "method", "module", "path"]
_FILE_LINE_COL_PARTS = 3
_FILE_LINE_PARTS = 2


class TargetSpecV1(CqStruct, frozen=True):
    """Parsed target spec for CQ bundles and neighborhood commands."""

    raw: str
    bundle_kind: BundleTargetKind | None = None
    bundle_value: str | None = None
    target_name: str | None = None
    target_file: str | None = None
    target_line: int | None = None
    target_col: int | None = None


def parse_target_spec(raw: str) -> TargetSpecV1:
    """Parse CQ target syntax shared by bundles and neighborhood commands.

    Supports three formats:
    - Bundle form: `kind:value` (e.g., `function:foo`, `module:bar`)
    - Anchor form: `file:line[:col]` (e.g., `src/foo.py:120`, `src/foo.py:120:4`)
    - Symbol form: bare symbol name (fallback)

    Parameters
    ----------
    raw
        Raw target specification string.

    Returns:
    -------
    TargetSpecV1
        Parsed target spec with normalized fields.
    """
    text = raw.strip()
    if not text:
        return TargetSpecV1(raw=raw)

    parts = text.split(":")

    if len(parts) >= _FILE_LINE_COL_PARTS and parts[-1].isdigit() and parts[-2].isdigit():
        file_part = ":".join(parts[:-2]).strip()
        if file_part:
            return TargetSpecV1(
                raw=raw,
                target_file=file_part,
                target_line=max(1, int(parts[-2])),
                target_col=max(0, int(parts[-1])),
            )

    if len(parts) >= _FILE_LINE_PARTS and parts[-1].isdigit():
        file_part = ":".join(parts[:-1]).strip()
        if file_part:
            return TargetSpecV1(
                raw=raw,
                target_file=file_part,
                target_line=max(1, int(parts[-1])),
            )

    if len(parts) == _FILE_LINE_PARTS:
        kind_part = parts[0].strip().lower()
        value_part = parts[1].strip()
        if kind_part in {"function", "class", "method", "module", "path"} and value_part:
            return TargetSpecV1(
                raw=raw,
                bundle_kind=kind_part,  # type: ignore[arg-type]
                bundle_value=value_part,
                target_name=value_part if kind_part != "path" else None,
            )

    return TargetSpecV1(raw=raw, target_name=text)


def parse_bundle_target_spec(raw: str) -> TargetSpecV1:
    """Parse and validate bundle target syntax (``kind:value``).

    Returns:
        Parsed target spec with required bundle fields populated.

    Raises:
        ValueError: If ``raw`` is not in ``kind:value`` format.
    """
    parsed = parse_target_spec(raw)
    if parsed.bundle_kind is None or parsed.bundle_value is None:
        msg = "Target spec must be in the form kind:value"
        raise ValueError(msg)
    return parsed


__all__ = [
    "BundleTargetKind",
    "TargetSpecV1",
    "parse_bundle_target_spec",
    "parse_target_spec",
]
