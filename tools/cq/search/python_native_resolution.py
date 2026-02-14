"""Native Python semantic resolution helpers for enrichment.

This module replaces the legacy CST resolution stage for byte-anchored
enrichment by using Python stdlib analysis (``ast`` + ``symtable``), optional
tree-sitter fallback context, and cached per-file analysis sessions.
"""

from __future__ import annotations

import ast
import symtable
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tools.cq.search.python_analysis_session import PythonAnalysisSession

_MAX_BINDINGS = 8
_MAX_QUALIFIED_NAME_CANDIDATES = 8
_MAX_SCOPE_TABLE_DEPTH = 24
_MAX_IMPORT_ALIAS_CHAIN_DEPTH = 12
_MAX_ENCLOSING_CONTEXT_DEPTH = 32

_NATIVE_RESOLUTION_ERRORS = (
    RuntimeError,
    TypeError,
    ValueError,
    AttributeError,
    UnicodeError,
    SyntaxError,
)


@dataclass(frozen=True, slots=True)
class _AstAnchor:
    node: ast.AST
    parents: tuple[ast.AST, ...]
    byte_start: int
    byte_end: int


@dataclass(frozen=True, slots=True)
class _DefinitionSite:
    kind: str
    byte_start: int | None


def _line_col_to_byte_offset(source_bytes: bytes, line: int, col: int) -> int | None:
    if line < 1 or col < 0:
        return None
    lines = source_bytes.splitlines(keepends=True)
    if line > len(lines):
        return None
    prefix = b"".join(lines[: line - 1])
    line_bytes = lines[line - 1]
    char_col = min(col, len(line_bytes.decode("utf-8", errors="replace")))
    as_bytes = line_bytes.decode("utf-8", errors="replace")[:char_col].encode(
        "utf-8", errors="replace"
    )
    return len(prefix) + len(as_bytes)


def _node_byte_span(node: ast.AST, source_bytes: bytes) -> tuple[int, int] | None:
    lineno = getattr(node, "lineno", None)
    col_offset = getattr(node, "col_offset", None)
    end_lineno = getattr(node, "end_lineno", None)
    end_col_offset = getattr(node, "end_col_offset", None)
    if not isinstance(lineno, int):
        return None
    if not isinstance(col_offset, int):
        return None
    if not isinstance(end_lineno, int):
        return None
    if not isinstance(end_col_offset, int):
        return None
    start = _line_col_to_byte_offset(
        source_bytes,
        lineno,
        col_offset,
    )
    end = _line_col_to_byte_offset(
        source_bytes,
        end_lineno,
        end_col_offset,
    )
    if start is None or end is None or end <= start:
        return None
    return start, end


def _ast_node_priority(node: ast.AST) -> int:
    if isinstance(node, ast.Name):
        return 0
    if isinstance(node, ast.Attribute):
        return 1
    if isinstance(node, ast.alias):
        return 2
    if isinstance(node, ast.Call):
        return 3
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return 4
    return 10


def _iter_nodes_with_parents(tree: ast.AST) -> Iterator[tuple[ast.AST, tuple[ast.AST, ...]]]:
    stack: list[tuple[ast.AST, tuple[ast.AST, ...]]] = [(tree, ())]
    while stack:
        node, parents = stack.pop()
        yield node, parents
        children = tuple(ast.iter_child_nodes(node))
        stack.extend((child, (*parents, node)) for child in reversed(children))


def _find_ast_anchor(
    tree: ast.AST,
    source_bytes: bytes,
    *,
    byte_start: int,
    byte_end: int,
) -> _AstAnchor | None:
    best: _AstAnchor | None = None
    best_key: tuple[int, int, int] | None = None
    for node, parents in _iter_nodes_with_parents(tree):
        span = _node_byte_span(node, source_bytes)
        if span is None:
            continue
        span_start, span_end = span
        contains_anchor = span_start <= byte_start and byte_end <= span_end
        overlaps_anchor = span_start < byte_end and byte_start < span_end
        if not contains_anchor and not overlaps_anchor:
            continue
        size = span_end - span_start
        depth = len(parents)
        priority = _ast_node_priority(node)
        candidate_key = (size, priority, -depth)
        if best_key is None or candidate_key < best_key:
            best = _AstAnchor(
                node=node,
                parents=parents,
                byte_start=span_start,
                byte_end=span_end,
            )
            best_key = candidate_key
    return best


def _extract_symbol_role(node: ast.AST) -> dict[str, object]:
    ctx = getattr(node, "ctx", None)
    if isinstance(ctx, ast.Load):
        return {"symbol_role": "read"}
    if isinstance(ctx, ast.Store):
        return {"symbol_role": "write"}
    if isinstance(ctx, ast.Del):
        return {"symbol_role": "delete"}
    if isinstance(
        node,
        (
            ast.FunctionDef,
            ast.AsyncFunctionDef,
            ast.ClassDef,
            ast.arg,
            ast.alias,
        ),
    ):
        return {"symbol_role": "write"}
    return {}


def _extract_enclosing_context(anchor: _AstAnchor) -> dict[str, object]:
    payload: dict[str, object] = {}
    enclosing_callable: str | None = None
    enclosing_class: str | None = None
    for depth, parent in enumerate(reversed(anchor.parents)):
        if depth >= _MAX_ENCLOSING_CONTEXT_DEPTH:
            break
        if enclosing_callable is None and isinstance(
            parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
        ):
            enclosing_callable = "<lambda>" if isinstance(parent, ast.Lambda) else parent.name
        if enclosing_class is None and isinstance(parent, ast.ClassDef):
            enclosing_class = parent.name
        if enclosing_callable is not None and enclosing_class is not None:
            break
    if enclosing_callable:
        payload["enclosing_callable"] = enclosing_callable
    if enclosing_class:
        payload["enclosing_class"] = enclosing_class
    return payload


def _simple_symbol_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.alias):
        return node.asname or node.name
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return node.name
    if isinstance(node, ast.arg):
        return node.arg
    return None


def _symbol_name_from_node(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call):
        return _symbol_name_from_node(node.func)
    if isinstance(node, ast.Attribute):
        return _attribute_to_text(node)
    return _simple_symbol_name(node)


def _attribute_to_text(node: ast.Attribute) -> str:
    parts: list[str] = [node.attr]
    current: ast.AST = node.value
    while True:
        if isinstance(current, ast.Name):
            parts.append(current.id)
            break
        if isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
            continue
        break
    parts.reverse()
    return ".".join(parts)


def _build_import_alias_map(tree: ast.AST) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name.split(".", 1)[0]
                alias_map[local_name] = alias.name
        elif isinstance(node, ast.ImportFrom):
            base = "." * node.level + (node.module or "")
            for alias in node.names:
                if alias.name == "*":
                    continue
                local_name = alias.asname or alias.name
                target = f"{base}.{alias.name}" if base else alias.name
                alias_map[local_name] = target
    return alias_map


def _site_with_kind(kind: str, node: ast.AST, source_bytes: bytes) -> _DefinitionSite:
    span = _node_byte_span(node, source_bytes)
    return _DefinitionSite(kind=kind, byte_start=(span[0] if span else None))


def _callable_or_class_definition_site(
    name: str, node: ast.AST, source_bytes: bytes
) -> _DefinitionSite | None:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
        return _site_with_kind("function_def", node, source_bytes)
    if isinstance(node, ast.ClassDef) and node.name == name:
        return _site_with_kind("class_def", node, source_bytes)
    return None


def _import_alias_local_name(alias: ast.alias, *, from_import: bool) -> str:
    if alias.asname:
        return alias.asname
    if from_import:
        return alias.name
    return alias.name.split(".", 1)[0]


def _matching_import_alias(name: str, node: ast.AST) -> ast.alias | None:
    if isinstance(node, ast.Import):
        for alias in node.names:
            if _import_alias_local_name(alias, from_import=False) == name:
                return alias
        return None
    if isinstance(node, ast.ImportFrom):
        for alias in node.names:
            if _import_alias_local_name(alias, from_import=True) == name:
                return alias
    return None


def _is_assignment_name(node: ast.AST, name: str) -> bool:
    return isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Store)


def _definition_site(name: str, node: ast.AST, source_bytes: bytes) -> _DefinitionSite | None:
    direct_site = _callable_or_class_definition_site(name, node, source_bytes)
    if direct_site is not None:
        return direct_site

    alias_site = _matching_import_alias(name, node)
    if alias_site is not None:
        return _site_with_kind("import_alias", alias_site, source_bytes)

    if isinstance(node, ast.arg) and node.arg == name:
        return _site_with_kind("parameter", node, source_bytes)

    if _is_assignment_name(node, name):
        return _site_with_kind("assignment", node, source_bytes)

    return None


def _build_definition_index(tree: ast.AST, source_bytes: bytes) -> dict[str, list[_DefinitionSite]]:
    index: dict[str, list[_DefinitionSite]] = {}
    for node in ast.walk(tree):
        # A placeholder name filters in _definition_site.
        for candidate_name in _candidate_definition_names(node):
            site = _definition_site(candidate_name, node, source_bytes)
            if site is None:
                continue
            rows = index.setdefault(candidate_name, [])
            if len(rows) < _MAX_BINDINGS:
                rows.append(site)
    return index


def _candidate_definition_names(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return (node.name,)
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        names: list[str] = []
        for alias in node.names:
            local_name = alias.asname or alias.name.split(".", 1)[0]
            names.append(local_name)
        return tuple(names)
    if isinstance(node, ast.arg):
        return (node.arg,)
    if isinstance(node, ast.Name):
        return (node.id,)
    return ()


def _scope_path_from_parents(parents: tuple[ast.AST, ...]) -> list[tuple[str, str, int]]:
    path: list[tuple[str, str, int]] = []
    for parent in parents:
        if isinstance(parent, ast.ClassDef):
            path.append(("class", parent.name, int(getattr(parent, "lineno", 0))))
        elif isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            path.append(("function", parent.name, int(getattr(parent, "lineno", 0))))
        elif isinstance(parent, ast.Lambda):
            path.append(("function", "<lambda>", int(getattr(parent, "lineno", 0))))
    return path


def _descend_scope_table(
    root: symtable.SymbolTable,
    path: list[tuple[str, str, int]],
) -> symtable.SymbolTable:
    table = root
    for depth, (scope_type, scope_name, scope_line) in enumerate(path):
        if depth >= _MAX_SCOPE_TABLE_DEPTH:
            break
        matches = [
            child
            for child in table.get_children()
            if child.get_type() == scope_type and child.get_name() == scope_name
        ]
        if not matches:
            break
        table = min(matches, key=lambda child: abs(child.get_lineno() - scope_line))
    return table


def _scope_table_chain(
    root: symtable.SymbolTable,
    path: list[tuple[str, str, int]],
) -> list[symtable.SymbolTable]:
    chain: list[symtable.SymbolTable] = [root]
    table = root
    for depth, (scope_type, scope_name, scope_line) in enumerate(path):
        if depth >= _MAX_SCOPE_TABLE_DEPTH:
            break
        matches = [
            child
            for child in table.get_children()
            if child.get_type() == scope_type and child.get_name() == scope_name
        ]
        if not matches:
            break
        table = min(matches, key=lambda child: abs(child.get_lineno() - scope_line))
        chain.append(table)
    return chain


def _binding_flags_for_name(table: symtable.SymbolTable, name: str) -> dict[str, bool] | None:
    try:
        symbol = table.lookup(name)
    except KeyError:
        return None
    return {
        "is_imported": symbol.is_imported(),
        "is_assigned": symbol.is_assigned(),
        "is_parameter": symbol.is_parameter(),
        "is_local": symbol.is_local(),
        "is_free": symbol.is_free(),
        "is_global": symbol.is_global(),
        "is_nonlocal": symbol.is_nonlocal(),
    }


def _resolve_symbol_table_and_flags(
    *,
    scope_tables: list[symtable.SymbolTable],
    name: str,
) -> tuple[symtable.SymbolTable | None, dict[str, bool] | None]:
    for table in reversed(scope_tables):
        flags = _binding_flags_for_name(table, name)
        if flags is None:
            continue
        # Prefer a parent declaration when a child scope marks the symbol as
        # implicit-global but doesn't carry imported metadata.
        if flags.get("is_global") and not flags.get("is_imported"):
            continue
        return table, flags

    for table in reversed(scope_tables):
        flags = _binding_flags_for_name(table, name)
        if flags is not None:
            return table, flags

    return None, None


def _extract_binding_candidates(
    *,
    anchor: _AstAnchor,
    scope_tables: list[symtable.SymbolTable],
    definition_index: dict[str, list[_DefinitionSite]],
) -> dict[str, object]:
    name = _symbol_name_from_node(anchor.node)
    if name is None:
        return {}
    resolved_table, flags = _resolve_symbol_table_and_flags(
        scope_tables=scope_tables,
        name=name,
    )
    if flags is None:
        return {}

    candidates: list[dict[str, object]] = []
    definitions = definition_index.get(name, [])
    if definitions:
        for site in definitions[:_MAX_BINDINGS]:
            row: dict[str, object] = {
                "name": name,
                "kind": site.kind,
                "byte_start": site.byte_start,
            }
            row.update(flags)
            if resolved_table is not None:
                row["scope"] = resolved_table.get_name()
                row["scope_type"] = resolved_table.get_type()
            candidates.append(row)
    else:
        row = {"name": name, "kind": "symbol"}
        row.update(flags)
        if resolved_table is not None:
            row["scope"] = resolved_table.get_name()
            row["scope_type"] = resolved_table.get_type()
        candidates.append(row)

    return {"binding_candidates": candidates[:_MAX_BINDINGS]}


def _append_alias_chain_entries(chain: list[dict[str, object]], alias: ast.alias) -> None:
    if alias.name:
        chain.append({"module": alias.name})
    if alias.asname:
        chain.append({"alias": alias.asname})


def _append_from_chain_entry(chain: list[dict[str, object]], from_node: ast.ImportFrom) -> None:
    prefix = "." * from_node.level
    module = from_node.module or ""
    value = f"{prefix}{module}" if (prefix or module) else "."
    chain.append({"from": value})


def _extract_import_alias_chain(anchor: _AstAnchor) -> dict[str, object]:
    chain: list[dict[str, object]] = []

    if isinstance(anchor.node, ast.alias):
        _append_alias_chain_entries(chain, anchor.node)

    for depth, parent in enumerate(reversed(anchor.parents)):
        if depth >= _MAX_IMPORT_ALIAS_CHAIN_DEPTH:
            break
        if isinstance(parent, ast.alias):
            _append_alias_chain_entries(chain, parent)
            continue
        if isinstance(parent, ast.ImportFrom):
            _append_from_chain_entry(chain, parent)
            break
        if isinstance(parent, ast.Import):
            break

    if chain:
        return {"import_alias_chain": chain}
    return {}


def _tree_sitter_anchor_text(
    source_bytes: bytes,
    *,
    tree: Any | None,
    byte_start: int,
    byte_end: int,
) -> str | None:
    if tree is None:
        return None
    root = getattr(tree, "root_node", None)
    if root is None or not hasattr(root, "named_descendant_for_byte_range"):
        return None
    probe_end = max(byte_start + 1, byte_end - 1)
    node = root.named_descendant_for_byte_range(byte_start, probe_end)
    if node is None:
        return None
    start = int(getattr(node, "start_byte", -1))
    end = int(getattr(node, "end_byte", -1))
    if start < 0 or end <= start or end > len(source_bytes):
        return None
    text = source_bytes[start:end].decode("utf-8", errors="replace").strip()
    return text or None


def _extract_qualified_name_candidates(
    *,
    anchor: _AstAnchor,
    alias_map: dict[str, str],
    enclosing_callable: str | None,
    enclosing_class: str | None,
    tree_sitter_text: str | None,
) -> dict[str, object]:
    raw_name = _symbol_name_from_node(anchor.node) or tree_sitter_text
    if raw_name is None:
        return {}
    candidate_rows: list[dict[str, object]] = [{"name": raw_name, "source": "syntax"}]

    root_name = raw_name.split(".", 1)[0]
    alias_target = alias_map.get(root_name)
    if alias_target is not None:
        if "." in raw_name:
            _, suffix = raw_name.split(".", 1)
            candidate_rows.append({"name": f"{alias_target}.{suffix}", "source": "import_alias"})
        else:
            candidate_rows.append({"name": alias_target, "source": "import_alias"})

    if enclosing_class:
        candidate_rows.append({"name": f"{enclosing_class}.{raw_name}", "source": "class_scope"})
    if enclosing_callable:
        candidate_rows.append({"name": f"{enclosing_callable}.{raw_name}", "source": "scope"})

    deduped: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for row in candidate_rows:
        name = row.get("name")
        source = row.get("source")
        if not isinstance(name, str) or not isinstance(source, str):
            continue
        key = (name, source)
        if key in seen:
            continue
        seen.add(key)
        deduped.append({"name": name, "source": source})
    deduped.sort(key=lambda item: (item["name"], item["source"]))
    if not deduped:
        return {}
    return {"qualified_name_candidates": deduped[:_MAX_QUALIFIED_NAME_CANDIDATES]}


def _build_resolution_index(
    tree: ast.AST,
    source_bytes: bytes,
) -> dict[str, object]:
    return {
        "import_alias_map": _build_import_alias_map(tree),
        "definition_index": _build_definition_index(tree, source_bytes),
    }


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

    from tools.cq.search.python_analysis_session import PythonAnalysisSession as _Session

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
        anchor = _find_ast_anchor(
            tree,
            source_bytes,
            byte_start=byte_start,
            byte_end=byte_end,
        )
        if anchor is None:
            return {}

        index = _load_resolution_index(
            tree=tree,
            source_bytes=source_bytes,
            session=typed_session,
        )
        alias_map = _coerce_alias_map(index.get("import_alias_map"))
        definition_index = _coerce_definition_index(index.get("definition_index"))

        scope_path = _scope_path_from_parents(anchor.parents)
        scope_table = _descend_scope_table(scope_root, scope_path)
        scope_tables = _scope_table_chain(scope_root, scope_path)
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
            source_bytes,
            tree=tree_sitter_tree,
            byte_start=byte_start,
            byte_end=byte_end,
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
    except _NATIVE_RESOLUTION_ERRORS:
        return {}
    else:
        return payload


__all__ = ["enrich_python_resolution_by_byte_range"]
