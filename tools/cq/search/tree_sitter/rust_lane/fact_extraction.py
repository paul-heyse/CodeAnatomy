"""Fact extraction from Rust query pack captures and rows.

This module extracts structured facts (definitions, references, calls,
imports, modules) from tree-sitter captures and evidence rows.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from tools.cq.core.locations import byte_offset_to_line_col
from tools.cq.search.rust.contracts import RustMacroExpansionRequestV1
from tools.cq.search.tree_sitter.contracts.core_models import ObjectEvidenceRowV1
from tools.cq.search.tree_sitter.core.node_utils import node_text

if TYPE_CHECKING:
    from tree_sitter import Node


_MAX_FIELDS_SHOWN = 8


def _capture_texts_from_captures(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
    *capture_names: str,
    limit: int = 8,
) -> list[str]:
    """Extract text from multiple capture names, deduplicating results.

    Parameters
    ----------
    captures
        Mapping of capture names to node lists.
    source_bytes
        Full source bytes.
    capture_names
        Capture names to extract text from.
    limit
        Maximum number of results to return.

    Returns:
    -------
    list[str]
        Deduplicated text values from captures.
    """
    out: list[str] = []
    seen: set[str] = set()
    for capture_name in capture_names:
        for captured in captures.get(capture_name, []):
            text = node_text(captured, source_bytes)
            if text is None or text in seen:
                continue
            seen.add(text)
            out.append(text)
            if len(out) >= limit:
                return out
    return out


def _rust_fact_lists(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    """Extract fact lists from tree-sitter captures.

    Parameters
    ----------
    captures
        Mapping of capture names to node lists.
    source_bytes
        Full source bytes.

    Returns:
    -------
    tuple[list[str], list[str], list[str], list[str], list[str]]
        Definitions, references, calls, imports, modules.
    """
    definitions = _capture_texts_from_captures(
        captures,
        source_bytes,
        "def.function.name",
        "def.struct.name",
        "def.enum.name",
        "def.trait.name",
        "def.module.name",
        "def.macro.name",
    )
    references = _capture_texts_from_captures(
        captures,
        source_bytes,
        "ref.identifier",
        "ref.scoped.name",
        "ref.use.path",
        "ref.macro.path",
    )
    calls = _capture_texts_from_captures(captures, source_bytes, "call.target", "call.macro.path")
    macro_calls = _capture_texts_from_captures(captures, source_bytes, "call.macro.path")
    for macro_name in macro_calls:
        normalized = macro_name if macro_name.endswith("!") else f"{macro_name}!"
        if normalized not in calls:
            calls.append(normalized)
    imports = _capture_texts_from_captures(
        captures,
        source_bytes,
        "import.path",
        "import.leaf",
        "import.extern.name",
    )
    modules = _capture_texts_from_captures(
        captures,
        source_bytes,
        "module.name",
        "def.module.name",
    )
    return definitions, references, calls, imports, modules


def _extend_fact_list(
    *,
    target: list[str],
    captures: Mapping[str, object],
    keys: tuple[str, ...],
) -> None:
    """Extend target list with captures from the given keys.

    Parameters
    ----------
    target
        List to extend.
    captures
        Mapping of capture names to values.
    keys
        Capture names to extract.
    """
    for key in keys:
        value = captures.get(key)
        if isinstance(value, str) and value and value not in target:
            target.append(value)


def _extend_macro_fact_list(
    *,
    target: list[str],
    captures: Mapping[str, object],
    key: str,
) -> None:
    """Extend target list with macro name, normalizing the '!' suffix.

    Parameters
    ----------
    target
        List to extend.
    captures
        Mapping of capture names to values.
    key
        Capture name for the macro.
    """
    value = captures.get(key)
    if not isinstance(value, str) or not value:
        return
    normalized = value if value.endswith("!") else f"{value}!"
    if normalized not in target:
        target.append(normalized)


def _extend_rust_fact_lists_from_rows(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    definitions: list[str],
    references: list[str],
    calls: list[str],
    imports: list[str],
    modules: list[str],
) -> None:
    """Extend fact lists from evidence rows.

    Parameters
    ----------
    rows
        Evidence rows from query execution.
    definitions
        Definitions list to extend.

    References:
        References list to extend.
    calls
        Calls list to extend.
    imports
        Imports list to extend.
    modules
        Modules list to extend.
    """
    for row in rows:
        if row.emit == "definitions":
            _extend_fact_list(
                target=definitions,
                captures=row.captures,
                keys=(
                    "def.function.name",
                    "def.struct.name",
                    "def.enum.name",
                    "def.trait.name",
                    "def.module.name",
                    "def.macro.name",
                ),
            )
        elif row.emit == "references":
            _extend_fact_list(
                target=references,
                captures=row.captures,
                keys=("ref.identifier", "ref.scoped.name", "ref.use.path", "ref.macro.path"),
            )
        elif row.emit == "calls":
            _extend_fact_list(
                target=calls,
                captures=row.captures,
                keys=("call.target",),
            )
            _extend_macro_fact_list(
                target=calls,
                captures=row.captures,
                key="call.macro.path",
            )
        elif row.emit == "imports":
            base = row.captures.get("import.base")
            leaf = row.captures.get("import.leaf")
            if isinstance(base, str) and isinstance(leaf, str):
                composed = f"{base}::{leaf}"
                if composed not in imports:
                    imports.append(composed)
            _extend_fact_list(
                target=imports,
                captures=row.captures,
                keys=("import.path", "import.leaf", "import.extern.name"),
            )
        elif row.emit == "modules":
            _extend_fact_list(
                target=modules,
                captures=row.captures,
                keys=("module.name", "def.module.name"),
            )


def _rust_fact_payload(
    *,
    definitions: list[str],
    references: list[str],
    calls: list[str],
    imports: list[str],
    modules: list[str],
) -> dict[str, list[str]]:
    """Build fact payload dict from lists.

    Parameters
    ----------
    definitions
        Definition names.

    References:
        Reference names.
    calls
        Call targets.
    imports
        Import paths.
    modules
        Module names.

    Returns:
    -------
    dict[str, list[str]]
        Fact payload with all lists capped at _MAX_FIELDS_SHOWN.
    """
    return {
        "definitions": definitions[:_MAX_FIELDS_SHOWN],
        "references": references[:_MAX_FIELDS_SHOWN],
        "calls": calls[:_MAX_FIELDS_SHOWN],
        "imports": imports[:_MAX_FIELDS_SHOWN],
        "modules": modules[:_MAX_FIELDS_SHOWN],
    }


def _module_rows_from_matches(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    file_key: str | None,
) -> list[dict[str, object]]:
    """Extract module rows from evidence rows.

    Parameters
    ----------
    rows
        Evidence rows from query execution.
    file_key
        File path for module rows.

    Returns:
    -------
    list[dict[str, object]]
        Module row dicts with module_id, module_name, file_path.
    """
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if row.emit != "modules":
            continue
        module_name = None
        for key in ("module.name", "def.module.name"):
            value = row.captures.get(key)
            if isinstance(value, str) and value:
                module_name = value
                break
        if module_name is None or module_name in seen:
            continue
        seen.add(module_name)
        out.append(
            {
                "module_id": f"module:{module_name}",
                "module_name": module_name,
                "file_path": file_key,
            }
        )
    return out


def _import_rows_from_matches(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    module_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Extract import rows from evidence rows.

    Parameters
    ----------
    rows
        Evidence rows from query execution.
    module_rows
        Module rows for source_module_id resolution.

    Returns:
    -------
    list[dict[str, object]]
        Import row dicts with source_module_id, target_path, visibility, is_reexport.
    """
    module_lookup = {
        str(row["module_name"]): str(row["module_id"])
        for row in module_rows
        if isinstance(row.get("module_name"), str) and isinstance(row.get("module_id"), str)
    }
    default_source = module_rows[0]["module_id"] if module_rows else "module:<root>"
    out: list[dict[str, object]] = []
    seen: set[tuple[str, str, str, bool]] = set()
    for row in rows:
        if row.emit != "imports":
            continue
        target_path = _import_target_path(row.captures)
        if target_path is None:
            continue
        visibility, is_reexport = _import_visibility(row.captures)
        source_module_id = _resolve_source_module_id(
            target_path=target_path,
            module_lookup=module_lookup,
            default_source=str(default_source),
        )
        key = (source_module_id, target_path, visibility, is_reexport)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "source_module_id": source_module_id,
                "target_path": target_path,
                "visibility": visibility,
                "is_reexport": is_reexport,
            }
        )
    return out


def _import_target_path(captures: Mapping[str, object]) -> str | None:
    base = captures.get("import.base")
    leaf = captures.get("import.leaf")
    if isinstance(base, str) and base and isinstance(leaf, str) and leaf:
        return f"{base}::{leaf}"
    for key in ("import.path", "import.extern.name"):
        value = captures.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _import_visibility(captures: Mapping[str, object]) -> tuple[str, bool]:
    visibility_capture = captures.get("import.visibility")
    visibility_text = visibility_capture if isinstance(visibility_capture, str) else ""
    visibility = "public" if visibility_text.startswith("pub") else "private"
    return visibility, visibility == "public"


def _resolve_source_module_id(
    *,
    target_path: str,
    module_lookup: Mapping[str, str],
    default_source: str,
) -> str:
    for module_name, module_id in module_lookup.items():
        if target_path.startswith(f"{module_name}::"):
            return module_id
    return default_source


def _macro_expansion_requests(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    source_bytes: bytes,
    file_key: str | None,
) -> tuple[RustMacroExpansionRequestV1, ...]:
    """Build macro expansion requests from evidence rows.

    Parameters
    ----------
    rows
        Evidence rows from query execution.
    source_bytes
        Full source bytes.
    file_key
        File path for requests.

    Returns:
    -------
    tuple[RustMacroExpansionRequestV1, ...]
        Macro expansion request structs.
    """
    file_path = file_key if isinstance(file_key, str) and file_key else "<memory>.rs"
    out: list[RustMacroExpansionRequestV1] = []
    seen: set[str] = set()
    for row in rows:
        if row.emit != "calls":
            continue
        macro_name = row.captures.get("call.macro.path")
        if not isinstance(macro_name, str) or not macro_name:
            continue
        line, col = byte_offset_to_line_col(source_bytes, row.anchor_start_byte)
        macro_call_id = f"{file_path}:{row.anchor_start_byte}:{row.anchor_end_byte}:{macro_name}"
        if macro_call_id in seen:
            continue
        seen.add(macro_call_id)
        out.append(
            RustMacroExpansionRequestV1(
                file_path=file_path,
                line=max(0, int(line) - 1),
                col=max(0, int(col)),
                macro_call_id=macro_call_id,
            )
        )
    return tuple(out)


__all__ = [
    "_extend_rust_fact_lists_from_rows",
    "_import_rows_from_matches",
    "_macro_expansion_requests",
    "_module_rows_from_matches",
    "_rust_fact_lists",
    "_rust_fact_payload",
]
