"""Python-ast/import tier helpers for enrichment runtime orchestration."""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING, cast

from tools.cq.search._shared.bounded_cache import BoundedCache
from tools.cq.search._shared.helpers import line_col_to_byte_offset, source_hash
from tools.cq.search.python.extractors_analysis import (
    extract_behavior_summary,
    extract_generator_flag,
    extract_import_detail,
    find_ast_function,
)
from tools.cq.search.python.extractors_classification import (
    _unwrap_decorated,
)
from tools.cq.search.python.extractors_runtime_astgrep import try_extract
from tools.cq.search.python.runtime_context import (
    ensure_python_cache_registered,
    get_default_python_runtime_context,
)

if TYPE_CHECKING:
    from ast_grep_py import SgNode


def python_ast_cache() -> BoundedCache[str, tuple[ast.Module, str]]:
    """Return process-default AST cache for Python enrichment.

    Returns:
        BoundedCache[str, tuple[ast.Module, str]]: Runtime AST cache.
    """
    ctx = get_default_python_runtime_context()
    ensure_python_cache_registered(ctx)
    return cast("BoundedCache[str, tuple[ast.Module, str]]", ctx.ast_cache)


def get_ast(source_bytes: bytes, *, cache_key: str) -> ast.Module | None:
    """Get or parse a cached Python AST module.

    Returns:
        ast.Module | None: Parsed AST module when available.
    """
    content_hash = source_hash(source_bytes)
    ast_cache = python_ast_cache()
    cached = ast_cache.get(cache_key)
    if cached is not None:
        cached_tree, cached_hash = cached
        if cached_hash == content_hash:
            return cached_tree
    try:
        tree = ast.parse(source_bytes)
    except SyntaxError:
        return None
    ast_cache.put(cache_key, (tree, content_hash))
    return tree


def enrich_python_ast_tier(
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
) -> tuple[dict[str, object], list[str]]:
    """Run Python-AST tier extractors for function nodes.

    Returns:
        tuple[dict[str, object], list[str]]: Extracted payload and degrade reasons.
    """
    payload: dict[str, object] = {}
    degrade_reasons: list[str] = []

    func_node = _unwrap_decorated(node)
    func_line = func_node.range().start.line + 1
    ast_tree = get_ast(source_bytes, cache_key=cache_key)
    if ast_tree is None:
        return payload, degrade_reasons

    func_ast = find_ast_function(ast_tree, func_line)
    if func_ast is not None:
        gen_result, gen_reason = try_extract("generator", extract_generator_flag, func_ast)
        if gen_result:
            payload["is_generator"] = gen_result.get("is_generator", False)
        if gen_reason:
            degrade_reasons.append(gen_reason)

        beh_result, beh_reason = try_extract("behavior", extract_behavior_summary, func_ast)
        payload.update(beh_result)
        if beh_reason:
            degrade_reasons.append(beh_reason)

    return payload, degrade_reasons


def enrich_import_tier(
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
    line: int,
) -> tuple[dict[str, object], list[str]]:
    """Run import-detail extraction for import statement nodes.

    Returns:
        tuple[dict[str, object], list[str]]: Import payload and degrade reasons.
    """
    degrade_reasons: list[str] = []
    ast_tree = get_ast(source_bytes, cache_key=cache_key)
    if ast_tree is None:
        return {}, degrade_reasons

    imp_result, imp_reason = try_extract(
        "import",
        extract_import_detail,
        node,
        source_bytes,
        ast_tree,
        line,
    )
    if imp_reason:
        degrade_reasons.append(imp_reason)
    return imp_result, degrade_reasons


def resolve_python_enrichment_range(
    *,
    node: SgNode,
    source_bytes: bytes,
    line: int,
    col: int,
    byte_start: int | None,
    byte_end: int | None,
) -> tuple[int | None, int | None]:
    """Resolve enrichment byte span from explicit/line-col/node fallbacks.

    Returns:
        tuple[int | None, int | None]: Resolved byte-span bounds.
    """
    resolved_start = byte_start
    if resolved_start is None:
        resolved_start = line_col_to_byte_offset(source_bytes, line, col)
    resolved_end = byte_end
    if resolved_end is None and resolved_start is not None:
        resolved_end = min(
            len(source_bytes),
            resolved_start + max(1, len(node.text().encode("utf-8"))),
        )
    return resolved_start, resolved_end


__all__ = [
    "enrich_import_tier",
    "enrich_python_ast_tier",
    "get_ast",
    "python_ast_cache",
    "resolve_python_enrichment_range",
]
