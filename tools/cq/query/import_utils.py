"""Shared import parsing helpers for query execution and symbol resolution."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParsedImportBinding:
    """Normalized import binding parsed from source text."""

    local_name: str
    source_module: str
    source_name: str | None
    is_from_import: bool


def strip_python_comment(text: str) -> str:
    """Strip one trailing Python inline comment and surrounding whitespace.

    Returns:
        str: Comment-stripped source text.
    """
    return text.split("#", maxsplit=1)[0].strip()


def extract_simple_import_name(text: str) -> str | None:
    """Extract name from ``import foo`` or ``import foo.bar``.

    Returns:
        str | None: Imported module path when matched.
    """
    cleaned = strip_python_comment(text)
    match = re.match(r"import\s+([\w.]+)", cleaned)
    return match.group(1) if match else None


def extract_simple_import_alias(text: str) -> str | None:
    """Extract alias from ``import foo as bar``.

    Returns:
        str | None: Alias name when present.
    """
    cleaned = strip_python_comment(text)
    match = re.match(r"import\s+[\w.]+\s+as\s+(\w+)", cleaned)
    return match.group(1) if match else None


def extract_from_import_name(text: str) -> str | None:
    """Extract imported name from ``from x import y`` when single-item.

    Returns:
        str | None: Imported symbol name when present.
    """
    cleaned = strip_python_comment(text)
    if "," not in cleaned:
        match = re.search(r"import\s+(\w+)\s*$", cleaned)
        if match:
            return match.group(1)
    return extract_from_module_name(cleaned)


def extract_from_import_alias(text: str) -> str | None:
    """Extract alias from ``from x import y as z``.

    Returns:
        str | None: Alias name when present.
    """
    cleaned = strip_python_comment(text)
    match = re.search(r"as\s+(\w+)\s*$", cleaned)
    return match.group(1) if match else None


def extract_from_module_name(text: str) -> str | None:
    """Extract module name from ``from x import ...``.

    Returns:
        str | None: Source module path when matched.
    """
    cleaned = strip_python_comment(text)
    match = re.match(r"from\s+([\w.]+)\s+import\s+", cleaned)
    return match.group(1) if match else None


def extract_rust_use_import_name(text: str) -> str | None:
    """Extract import target from Rust ``use`` declarations.

    Returns:
        str | None: Local symbol name imported by the `use` declaration.
    """
    cleaned = text.split("//", maxsplit=1)[0].strip()
    match = re.match(r"use\s+([^;]+);?", cleaned)
    if not match:
        return None
    use_target = match.group(1).strip()
    if " as " in use_target:
        return use_target.rsplit(" as ", maxsplit=1)[1].strip()
    return use_target.rsplit("::", maxsplit=1)[-1].strip("{} ").strip()


def parse_import_bindings(text: str) -> tuple[ParsedImportBinding, ...]:
    """Parse ``import``/``from ... import`` statement text into bindings.

    Returns:
        tuple[ParsedImportBinding, ...]: Normalized binding tuples.
    """
    stripped = strip_python_comment(text)
    if stripped.startswith("import "):
        return _parse_import_stmt(stripped)
    if stripped.startswith("from "):
        return _parse_from_import_stmt(stripped)
    return ()


def _parse_import_stmt(text: str) -> tuple[ParsedImportBinding, ...]:
    remainder = text[7:].strip()
    if not remainder:
        return ()
    if " as " in remainder and "," not in remainder:
        source, alias = remainder.split(" as ", 1)
        source = source.strip()
        alias = alias.strip()
        if not source or not alias:
            return ()
        return (
            ParsedImportBinding(
                local_name=alias,
                source_module=source,
                source_name=source.split(".")[-1],
                is_from_import=False,
            ),
        )

    bindings: list[ParsedImportBinding] = []
    for module_name in remainder.split(","):
        cleaned_module = module_name.strip()
        if not cleaned_module:
            continue
        if " as " in cleaned_module:
            source, alias = cleaned_module.split(" as ", 1)
            source = source.strip()
            alias = alias.strip()
            if not source or not alias:
                continue
            bindings.append(
                ParsedImportBinding(
                    local_name=alias,
                    source_module=source,
                    source_name=source.split(".")[-1],
                    is_from_import=False,
                )
            )
            continue
        bindings.append(
            ParsedImportBinding(
                local_name=cleaned_module.split(".")[-1],
                source_module=cleaned_module,
                source_name=None,
                is_from_import=False,
            )
        )
    return tuple(bindings)


def _parse_from_import_stmt(text: str) -> tuple[ParsedImportBinding, ...]:
    match = re.match(r"from\s+([\w.]+)\s+import\s+(.+)", text)
    if not match:
        return ()
    source_module = match.group(1)
    names_part = match.group(2).strip()
    if names_part.startswith("("):
        names_part = names_part[1:]
    if names_part.endswith(")"):
        names_part = names_part[:-1]

    bindings: list[ParsedImportBinding] = []
    for spec in names_part.split(","):
        cleaned_spec = spec.strip()
        if not cleaned_spec:
            continue
        if " as " in cleaned_spec:
            source_name, local_name = (part.strip() for part in cleaned_spec.split(" as ", 1))
        else:
            source_name = cleaned_spec
            local_name = cleaned_spec
        if not local_name or not source_name:
            continue
        bindings.append(
            ParsedImportBinding(
                local_name=local_name,
                source_module=source_module,
                source_name=source_name if source_name != local_name else None,
                is_from_import=True,
            )
        )
    return tuple(bindings)


__all__ = [
    "ParsedImportBinding",
    "extract_from_import_alias",
    "extract_from_import_name",
    "extract_from_module_name",
    "extract_rust_use_import_name",
    "extract_simple_import_alias",
    "extract_simple_import_name",
    "parse_import_bindings",
    "strip_python_comment",
]
