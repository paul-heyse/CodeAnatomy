"""Contract and catalog type-string normalization helpers."""

from __future__ import annotations


def extract_inner_type(text: str, open_char: str, close_char: str) -> str:
    """Return the substring enclosed by the first matching delimiter pair."""
    start = text.find(open_char)
    if start < 0:
        return ""
    depth = 0
    for idx in range(start, len(text)):
        ch = text[idx]
        if ch == open_char:
            depth += 1
        elif ch == close_char:
            depth -= 1
            if depth == 0:
                return text[start + 1 : idx]
    return text[start + 1 :]


def split_top_level_types(text: str) -> list[str]:
    """Split comma-separated types while preserving nested type expressions.

    Returns:
        list[str]: Top-level type segments without splitting nested expressions.
    """
    parts: list[str] = []
    depth = 0
    start = 0
    for idx, ch in enumerate(text):
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth = max(depth - 1, 0)
        elif ch == "," and depth == 0:
            parts.append(text[start:idx])
            start = idx + 1
    tail = text[start:]
    if tail:
        parts.append(tail)
    return parts


def strip_type_prefix(text: str) -> str:
    """Drop list/map/struct item labels from nested type expressions.

    Returns:
        str: Type expression without structural item prefixes.
    """
    if ":" in text:
        prefix, rest = text.split(":", 1)
        if prefix in {"item", "field", "key", "value", "element"}:
            return rest
    return text


def normalize_contract_type(text: str) -> str:
    """Normalize nested contract type text into canonical comparison form.

    Returns:
        str: Canonicalized contract type string for comparison.
    """
    if text.startswith(("list(", "list<")):
        open_char = "(" if text.startswith("list(") else "<"
        close_char = ")" if open_char == "(" else ">"
        inner = extract_inner_type(text, open_char, close_char)
        parts = split_top_level_types(inner)
        if parts:
            inner = parts[0]
        inner = strip_type_prefix(inner)
        return f"list<{normalize_contract_type(inner)}>"
    if text.startswith(("map(", "map<")):
        return "map"
    if text.startswith(("struct(", "struct<")):
        return "struct"
    return text


def normalize_type_string(value: str) -> str:
    """Normalize a contract or catalog type string for equality checks.

    Returns:
    -------
    str
        Canonical normalized type string.
    """
    normalized = value.lower().replace(" ", "")
    normalized = normalized.replace("largeutf8", "string")
    normalized = normalized.replace("utf8", "string")
    normalized = normalized.replace("non-null", "")
    normalized = normalized.replace("nonnull", "")
    normalized = normalized.replace("'", "").replace('"', "")
    return normalize_contract_type(normalized)


__all__ = [
    "extract_inner_type",
    "normalize_contract_type",
    "normalize_type_string",
    "split_top_level_types",
    "strip_type_prefix",
]
