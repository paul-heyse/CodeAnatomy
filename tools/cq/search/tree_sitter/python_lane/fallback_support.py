"""Fallback and alias-chain helper functions for Python tree-sitter enrichment."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.core.infrastructure import child_by_field
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.python_lane.constants import (
    MAX_CAPTURE_ITEMS as _MAX_CAPTURE_ITEMS,
)
from tools.cq.search.tree_sitter.python_lane.constants import (
    get_python_field_ids,
)

if TYPE_CHECKING:
    from tree_sitter import Node

_MAX_CAPTURE_TEXT_LEN = 120


def capture_named_definition(
    name_nodes: list[Node],
    fallback_nodes: list[Node],
    source_bytes: bytes,
) -> str | None:
    """Capture a definition name from direct captures or fallback nodes.

    Returns:
        str | None: Captured definition name when available.
    """
    if name_nodes:
        name = node_text(name_nodes[0], source_bytes)
        if name:
            return name
    if not fallback_nodes:
        return None
    name_node = child_by_field(fallback_nodes[0], "name", get_python_field_ids())
    if name_node is None:
        return None
    name = node_text(name_node, source_bytes)
    return name or None


def _capture_text_rows(
    captures: dict[str, list[Node]],
    capture_names: tuple[str, ...],
    source_bytes: bytes,
) -> list[str]:
    rows: list[str] = []
    for name in capture_names:
        nodes = captures.get(name, [])
        for node in nodes:
            text = node_text(node, source_bytes)
            if text:
                rows.append(text[:_MAX_CAPTURE_TEXT_LEN])
            if len(rows) >= _MAX_CAPTURE_ITEMS:
                return rows
    return rows


def _append_chain_row(chain: list[dict[str, object]], *, key: str, value: str) -> None:
    if not value or len(chain) >= _MAX_CAPTURE_ITEMS:
        return
    row: dict[str, object] = {key: value[:_MAX_CAPTURE_TEXT_LEN]}
    if row not in chain:
        chain.append(row)


def _append_named_import_chain(
    chain: list[dict[str, object]],
    name_node: Node,
    source_bytes: bytes,
) -> None:
    if getattr(name_node, "type", "") != "aliased_import":
        _append_chain_row(chain, key="module", value=node_text(name_node, source_bytes))
        return

    module_node = child_by_field(name_node, "name", get_python_field_ids())
    alias_node = child_by_field(name_node, "alias", get_python_field_ids())
    if module_node is not None:
        _append_chain_row(chain, key="module", value=node_text(module_node, source_bytes))
    if alias_node is not None:
        _append_chain_row(chain, key="alias", value=node_text(alias_node, source_bytes))


def _append_import_statement_chain(
    chain: list[dict[str, object]],
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> None:
    for statement in captures.get("import.statement", []):
        name_node = child_by_field(statement, "name", get_python_field_ids())
        if name_node is None:
            continue
        _append_named_import_chain(chain, name_node, source_bytes)
        if len(chain) >= _MAX_CAPTURE_ITEMS:
            return


def _append_import_from_statement_chain(
    chain: list[dict[str, object]],
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> None:
    for statement in captures.get("import.from_statement", []):
        module_node = child_by_field(statement, "module_name", get_python_field_ids())
        if module_node is not None:
            _append_chain_row(chain, key="from", value=node_text(module_node, source_bytes))
        name_node = child_by_field(statement, "name", get_python_field_ids())
        if name_node is not None:
            _append_named_import_chain(chain, name_node, source_bytes)
        if len(chain) >= _MAX_CAPTURE_ITEMS:
            return


def _append_module_alias_pairs(
    chain: list[dict[str, object]],
    *,
    modules: list[str],
    aliases: list[str],
) -> None:
    for index, module_name in enumerate(modules[:_MAX_CAPTURE_ITEMS]):
        _append_chain_row(chain, key="module", value=module_name)
        if index < len(aliases):
            _append_chain_row(chain, key="alias", value=aliases[index])
        if len(chain) >= _MAX_CAPTURE_ITEMS:
            return


def _append_alias_only_rows(
    chain: list[dict[str, object]],
    *,
    aliases: list[str],
) -> None:
    for alias in aliases:
        _append_chain_row(chain, key="alias", value=alias)
        if len(chain) >= _MAX_CAPTURE_ITEMS:
            return


def capture_import_alias_chain(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> list[dict[str, object]]:
    """Capture import alias chain rows from parse captures.

    Returns:
        list[dict[str, object]]: Ordered import-alias chain rows.
    """
    chain: list[dict[str, object]] = []
    _append_import_statement_chain(chain, captures, source_bytes)
    _append_import_from_statement_chain(chain, captures, source_bytes)

    from_modules = _capture_text_rows(captures, ("import.from.module",), source_bytes)
    modules = _capture_text_rows(
        captures,
        ("import.module", "import.from.name"),
        source_bytes,
    )
    aliases = _capture_text_rows(
        captures,
        ("import.alias", "import.from.alias"),
        source_bytes,
    )
    if from_modules:
        _append_chain_row(chain, key="from", value=from_modules[0])

    _append_module_alias_pairs(chain, modules=modules, aliases=aliases)

    if not modules:
        _append_alias_only_rows(chain, aliases=aliases)
    return chain[:_MAX_CAPTURE_ITEMS]


def capture_binding_candidates(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> list[dict[str, object]]:
    """Capture binding candidates from assignment/identifier captures.

    Returns:
        list[dict[str, object]]: Binding candidate rows.
    """
    candidates: list[dict[str, object]] = []
    for capture_name, kind in (
        ("assignment.target", "assignment"),
        ("binding.identifier", "identifier"),
    ):
        nodes = captures.get(capture_name, [])
        for node in nodes:
            text = node_text(node, source_bytes)
            if not text:
                continue
            row: dict[str, object] = {
                "name": text[:_MAX_CAPTURE_TEXT_LEN],
                "kind": f"tree_sitter_{kind}",
                "byte_start": int(getattr(node, "start_byte", -1)),
            }
            candidates.append(row)
            if len(candidates) >= _MAX_CAPTURE_ITEMS:
                return candidates
    return candidates


def capture_qualified_name_candidates(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> list[dict[str, object]]:
    """Capture qualified-name candidates from parse captures.

    Returns:
        list[dict[str, object]]: Qualified-name candidate rows.
    """
    rows = _capture_text_rows(
        captures,
        (
            "call.target.identifier",
            "call.target.attribute",
            "call.function.identifier",
            "call.function.attribute",
            "ref.identifier",
            "ref.attribute",
            "import.module",
            "import.from.name",
            "def.function.name",
            "class.definition.name",
            "attribute.expr",
        ),
        source_bytes,
    )
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for text in rows:
        if text in seen:
            continue
        seen.add(text)
        out.append({"name": text, "source": "tree_sitter"})
        if len(out) >= _MAX_CAPTURE_ITEMS:
            break
    return out


def default_parse_quality() -> dict[str, object]:
    """Return a default parse-quality payload for non-error states."""
    return {
        "has_error": False,
        "error_nodes": list[str](),
        "missing_nodes": list[str](),
        "did_exceed_match_limit": False,
    }


def _byte_to_char_index(source_bytes: bytes, byte_offset: int) -> int:
    safe_offset = max(0, min(byte_offset, len(source_bytes)))
    return len(source_bytes[:safe_offset].decode("utf-8", errors="replace"))


def _extract_import_alias_rows(
    source: str,
    *,
    token: str,
) -> tuple[list[dict[str, object]], str | None]:
    rows: list[dict[str, object]] = []
    module_for_token: str | None = None
    for line in source.splitlines():
        stripped = line.strip()
        if not stripped.startswith("import ") or " as " not in stripped:
            continue
        left, alias = stripped[7:].split(" as ", 1)
        alias_name = alias.strip()
        if alias_name != token:
            continue
        module_name = left.strip()
        rows.append({"module": module_name, "alias": alias_name})
        module_for_token = module_name
    return rows, module_for_token


def _line_text_at_offset(source: str, *, char_offset: int) -> str:
    line_start = source.rfind("\n", 0, char_offset) + 1
    line_end = source.find("\n", char_offset)
    if line_end < 0:
        line_end = len(source)
    return source[line_start:line_end]


def _resolve_call_target(line_text: str, *, token: str) -> str | None:
    marker = f"{token}."
    marker_pos = line_text.find(marker)
    if marker_pos < 0:
        return None
    call_tail = line_text[marker_pos:].strip()
    open_paren = call_tail.find("(")
    if open_paren <= 0:
        return None
    return call_tail[:open_paren].strip()


def _fallback_qualified_candidates(
    *,
    call_target: str | None,
    module_for_token: str | None,
) -> list[dict[str, object]]:
    qualified: list[dict[str, object]] = []
    if call_target:
        qualified.append({"name": call_target})
    if module_for_token:
        qualified.append({"name": module_for_token})
    if module_for_token and call_target and "." in call_target:
        suffix = call_target.split(".", 1)[1]
        qualified.append({"name": f"{module_for_token}.{suffix}"})
    return qualified


def _fallback_binding_candidates(
    *,
    token: str,
    call_target: str | None,
    module_for_token: str | None,
) -> list[dict[str, object]]:
    if module_for_token:
        return [{"name": token, "symbol_role": "import_alias", "module": module_for_token}]
    if call_target:
        return [{"name": token, "symbol_role": "reference"}]
    return []


def fallback_resolution_fields(
    *,
    source: str,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
) -> dict[str, object]:
    """Build fallback resolution payload when query captures are insufficient.

    Returns:
        dict[str, object]: Best-effort fallback semantic payload.
    """
    token = source_bytes[byte_start:byte_end].decode("utf-8", errors="replace").strip()
    if not token:
        return {}

    char_start = _byte_to_char_index(source_bytes, byte_start)
    alias_rows, module_for_token = _extract_import_alias_rows(source, token=token)
    line_text = _line_text_at_offset(source, char_offset=char_start)
    call_target = _resolve_call_target(line_text, token=token)
    qualified = _fallback_qualified_candidates(
        call_target=call_target,
        module_for_token=module_for_token,
    )
    binding = _fallback_binding_candidates(
        token=token,
        call_target=call_target,
        module_for_token=module_for_token,
    )

    out: dict[str, object] = {}
    if alias_rows:
        out["import_alias_chain"] = alias_rows
    if call_target:
        out["call_target"] = call_target
    if qualified:
        out["qualified_name_candidates"] = qualified
    if binding:
        out["binding_candidates"] = binding
    return out


__all__ = [
    "capture_binding_candidates",
    "capture_import_alias_chain",
    "capture_named_definition",
    "capture_qualified_name_candidates",
    "default_parse_quality",
    "fallback_resolution_fields",
]
