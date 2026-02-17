"""AST-grep metavariable pattern helper utilities."""

from __future__ import annotations

import re

_METAVAR_TOKEN_RE = re.compile(r"\${1,3}([A-Z][A-Z0-9_]*)")
_VARIADIC_METAVAR_TOKEN_RE = re.compile(r"\${3}([A-Z][A-Z0-9_]*)")


def extract_metavar_names(text: str) -> tuple[str, ...]:
    """Extract unique metavariable names from one pattern string.

    Returns:
        tuple[str, ...]: Sorted unique metavariable names.
    """
    if not text:
        return ()
    return tuple(sorted({match.group(1) for match in _METAVAR_TOKEN_RE.finditer(text)}))


def extract_variadic_metavar_names(text: str) -> tuple[str, ...]:
    """Extract unique variadic metavariable names from one pattern string.

    Returns:
        tuple[str, ...]: Sorted unique variadic metavariable names.
    """
    if not text:
        return ()
    return tuple(sorted({match.group(1) for match in _VARIADIC_METAVAR_TOKEN_RE.finditer(text)}))


__all__ = ["extract_metavar_names", "extract_variadic_metavar_names"]
