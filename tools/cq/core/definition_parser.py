"""Shared definition and symbol parsing helpers for Python and Rust text."""

from __future__ import annotations

import re

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_RUST_VISIBILITY = r"(?:pub(?:\([^)]*\))?\s+)?"
_RUST_FN_PREFIX = (
    rf"{_RUST_VISIBILITY}(?:async\s+)?(?:const\s+)?(?:unsafe\s+)?"
    rf"(?:extern(?:\s+\"[^\"]+\")?\s+)?fn\s+"
)
_RUST_TYPE_PREFIX = rf"{_RUST_VISIBILITY}(?:struct|enum|trait|union)\s+"
_RUST_MOD_PREFIX = rf"{_RUST_VISIBILITY}mod\s+"
_RUST_IMPL_PREFIX = rf"{_RUST_VISIBILITY}impl(?:<[^>]*>\s*)?\s+"

_PY_DEF_RE = re.compile(rf"^(?:async\s+def|def)\s+({_IDENT})\b")
_PY_CLASS_RE = re.compile(rf"^class\s+({_IDENT})\b")
_RUST_FN_RE = re.compile(rf"^{_RUST_FN_PREFIX}({_IDENT})\b")
_RUST_TYPE_RE = re.compile(rf"^{_RUST_TYPE_PREFIX}({_IDENT})\b")
_RUST_MOD_RE = re.compile(rf"^{_RUST_MOD_PREFIX}({_IDENT})\b")
_RUST_IMPL_FOR_RE = re.compile(
    rf"^{_RUST_IMPL_PREFIX}(?:{_IDENT}(?:::{_IDENT})*\s+for\s+)?({_IDENT})\b"
)


def extract_definition_name(text: str) -> str | None:
    """Extract definition name from a Python or Rust source line.

    Returns:
        Parsed definition symbol when available, else ``None``.
    """
    stripped = text.strip()
    if not stripped:
        return None

    for pattern in (_PY_DEF_RE, _PY_CLASS_RE, _RUST_FN_RE, _RUST_TYPE_RE, _RUST_MOD_RE):
        match = pattern.match(stripped)
        if match is not None:
            return match.group(1)

    impl_match = _RUST_IMPL_FOR_RE.match(stripped)
    if impl_match is not None:
        return impl_match.group(1)
    return None


def extract_definition_kind(text: str) -> str | None:
    """Return normalized definition kind for a Python or Rust source line.

    Returns:
        ``function``, ``class``, ``type``, ``module``, or ``None``.
    """
    stripped = text.strip()
    if not stripped:
        return None
    if _PY_DEF_RE.match(stripped) or _RUST_FN_RE.match(stripped):
        return "function"
    if _PY_CLASS_RE.match(stripped):
        return "class"
    if _RUST_TYPE_RE.match(stripped) or _RUST_IMPL_FOR_RE.match(stripped):
        return "type"
    if _RUST_MOD_RE.match(stripped):
        return "module"
    return None


def is_definition_like_text(text: str) -> bool:
    """Return whether a source line is definition-like.

    Returns:
        ``True`` when definition parsing succeeds.
    """
    return extract_definition_name(text) is not None


def extract_symbol_name(text: str, *, fallback: str = "") -> str:
    """Extract best-effort symbol name from definition or call-like text.

    Returns:
        Parsed symbol name, or ``fallback`` when no name can be derived.
    """
    definition_name = extract_definition_name(text)
    if definition_name:
        return definition_name

    raw = text.strip()
    if not raw:
        return fallback
    if "(" in raw:
        call_head = raw.split("(", 1)[0].strip()
        if "." in call_head:
            return call_head.rsplit(".", 1)[-1]
        if "::" in call_head:
            return call_head.rsplit("::", 1)[-1]
        return call_head
    return raw if raw else fallback


__all__ = [
    "extract_definition_kind",
    "extract_definition_name",
    "extract_symbol_name",
    "is_definition_like_text",
]
