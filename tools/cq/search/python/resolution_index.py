"""Native Python semantic resolution orchestration for enrichment."""

from __future__ import annotations

import ast
import symtable
from typing import TYPE_CHECKING, Any

from tools.cq.search.python.resolution_support import (
    _MAX_BINDINGS,
    _NATIVE_RESOLUTION_ERRORS,
    _AstAnchor,
    _build_resolution_index,
    _DefinitionSite,
    _descend_scope_table,
    _extract_binding_candidates,
    _extract_enclosing_context,
    _extract_import_alias_chain,
    _extract_qualified_name_candidates,
    _extract_symbol_role,
    _find_ast_anchor,
    _find_ast_anchor_from_index,
    _ResolutionPayloadInputs,
    _scope_path_from_parents,
    _scope_table_chain,
    _tree_sitter_anchor_text,
)

if TYPE_CHECKING:
    from tools.cq.search.python.analysis_session import PythonAnalysisSession


def _coerce_alias_map(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        key: target
        for key, target in value.items()
        if isinstance(key, str) and isinstance(target, str)
    }


def _coerce_definition_index(value: object) -> dict[str, list[_DefinitionSite]]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, list[_DefinitionSite]] = {}
    for key, rows in value.items():
        if not isinstance(key, str) or not isinstance(rows, list):
            continue
        typed_rows = [site for site in rows if isinstance(site, _DefinitionSite)]
        if typed_rows:
            out[key] = typed_rows[:_MAX_BINDINGS]
    return out


def _as_optional_str(value: object | None) -> str | None:
    return value if isinstance(value, str) else None


def _load_resolution_index(
    *,
    tree: ast.AST,
    source_bytes: bytes,
    session: PythonAnalysisSession | None,
) -> dict[str, object]:
    if session is None:
        return _build_resolution_index(tree, source_bytes)
    cached = session.ensure_resolution_index()
    if cached is not None:
        return cached
    built = _build_resolution_index(tree, source_bytes)
    session.resolution_index = built
    return built


def _load_ast_symtable_tree(
    *,
    source: str,
    source_bytes: bytes,
    file_path: str,
    session: PythonAnalysisSession | None,
) -> tuple[ast.AST, symtable.SymbolTable, Any | None] | None:
    if session is not None:
        tree = session.ensure_ast()
        scope_table = session.ensure_symtable()
        ts_tree = session.ensure_tree_sitter_tree()
        if tree is None or scope_table is None:
            return None
        return tree, scope_table, ts_tree
    try:
        tree = ast.parse(source_bytes)
        scope_table = symtable.symtable(source, file_path, "exec")
    except _NATIVE_RESOLUTION_ERRORS:
        return None
    return tree, scope_table, None


def _resolve_ast_anchor(
    *,
    tree: ast.AST,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
    session: PythonAnalysisSession | None,
) -> _AstAnchor | None:
    anchor: _AstAnchor | None = None
    if session is not None:
        span_index = session.ensure_ast_span_index()
        if span_index:
            anchor = _find_ast_anchor_from_index(
                span_index,
                byte_start=byte_start,
                byte_end=byte_end,
            )
    if anchor is not None:
        return anchor
    return _find_ast_anchor(
        tree,
        source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
    )


def _build_resolution_payload(
    *,
    anchor: _AstAnchor,
    inputs: _ResolutionPayloadInputs,
) -> dict[str, object]:
    index = _load_resolution_index(
        tree=inputs.tree,
        source_bytes=inputs.source_bytes,
        session=inputs.session,
    )
    alias_map = _coerce_alias_map(index.get("import_alias_map"))
    definition_index = _coerce_definition_index(index.get("definition_index"))

    scope_path = _scope_path_from_parents(anchor.parents)
    scope_table = _descend_scope_table(inputs.scope_root, scope_path)
    scope_tables = _scope_table_chain(inputs.scope_root, scope_path)
    if scope_table not in scope_tables:
        scope_tables.append(scope_table)

    payload: dict[str, object] = {}
    payload.update(_extract_symbol_role(anchor.node))
    enclosing = _extract_enclosing_context(anchor)
    payload.update(enclosing)
    payload.update(_extract_import_alias_chain(anchor))
    payload.update(
        _extract_binding_candidates(
            anchor=anchor,
            scope_tables=scope_tables,
            definition_index=definition_index,
        )
    )

    tree_sitter_text = _tree_sitter_anchor_text(
        inputs.source_bytes,
        tree=inputs.tree_sitter_tree,
        byte_start=inputs.byte_start,
        byte_end=inputs.byte_end,
    )
    payload.update(
        _extract_qualified_name_candidates(
            anchor=anchor,
            alias_map=alias_map,
            enclosing_callable=_as_optional_str(enclosing.get("enclosing_callable")),
            enclosing_class=_as_optional_str(enclosing.get("enclosing_class")),
            tree_sitter_text=tree_sitter_text,
        )
    )
    return payload


def enrich_python_resolution_by_byte_range(
    source: str,
    *,
    source_bytes: bytes,
    file_path: str,
    byte_start: int,
    byte_end: int,
    session: object | None = None,
) -> dict[str, object]:
    """Extract native semantic context for a byte-anchored match.

    Returns:
    -------
    dict[str, object]
        Native Python resolution payload fields.
    """
    if byte_start < 0 or byte_end <= byte_start or byte_end > len(source_bytes):
        return {}

    from tools.cq.search.python.analysis_session import PythonAnalysisSession as _Session

    typed_session: PythonAnalysisSession | None = session if isinstance(session, _Session) else None

    loaded = _load_ast_symtable_tree(
        source=source,
        source_bytes=source_bytes,
        file_path=file_path,
        session=typed_session,
    )
    if loaded is None:
        return {}
    tree, scope_root, tree_sitter_tree = loaded

    try:
        anchor = _resolve_ast_anchor(
            tree=tree,
            source_bytes=source_bytes,
            byte_start=byte_start,
            byte_end=byte_end,
            session=typed_session,
        )
        if anchor is None:
            return {}
        payload = _build_resolution_payload(
            anchor=anchor,
            inputs=_ResolutionPayloadInputs(
                tree=tree,
                source_bytes=source_bytes,
                scope_root=scope_root,
                tree_sitter_tree=tree_sitter_tree,
                session=typed_session,
                byte_start=byte_start,
                byte_end=byte_end,
            ),
        )
    except _NATIVE_RESOLUTION_ERRORS:
        return {}
    else:
        return payload


def build_resolution_index(
    *,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
    cache_key: str,
    session: object | None = None,
) -> dict[str, object]:
    """Compatibility wrapper for native-resolution index building.

    Returns:
        dict[str, object]: Function return value.
    """
    return enrich_python_resolution_by_byte_range(
        source=source_bytes.decode("utf-8", errors="replace"),
        source_bytes=source_bytes,
        file_path=cache_key,
        byte_start=byte_start,
        byte_end=byte_end,
        session=session,
    )


__all__ = ["build_resolution_index", "enrich_python_resolution_by_byte_range"]
