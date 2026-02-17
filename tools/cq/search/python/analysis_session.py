"""Per-file Python analysis session cache for enrichment pipelines.

This module keeps analysis artifacts scoped to one file+content hash to avoid
duplicate parse work when multiple matches occur in the same file.
"""

from __future__ import annotations

import ast
import symtable
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from types import CodeType
from typing import TYPE_CHECKING, Any

from tools.cq.search._shared.bounded_cache import BoundedCache
from tools.cq.search._shared.helpers import source_hash
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.python.ast_utils import ast_node_priority, node_byte_span

if TYPE_CHECKING:
    from ast_grep_py import SgRoot

_MAX_SESSION_CACHE_ENTRIES = 64
_SESSION_CACHE: BoundedCache[str, PythonAnalysisSession] = BoundedCache(
    max_size=_MAX_SESSION_CACHE_ENTRIES, policy="fifo"
)


@dataclass(frozen=True, slots=True)
class AstSpanEntry:
    """Precomputed AST span entry for fast byte-anchor lookup."""

    node: ast.AST
    parents: tuple[ast.AST, ...]
    byte_start: int
    byte_end: int
    priority: int


def _iter_nodes_with_parents(tree: ast.AST) -> list[tuple[ast.AST, tuple[ast.AST, ...]]]:
    nodes: list[tuple[ast.AST, tuple[ast.AST, ...]]] = []
    stack: list[tuple[ast.AST, tuple[ast.AST, ...]]] = [(tree, ())]
    while stack:
        node, parents = stack.pop()
        nodes.append((node, parents))
        children = tuple(ast.iter_child_nodes(node))
        stack.extend((child, (*parents, node)) for child in reversed(children))
    return nodes


@dataclass(slots=True)
class PythonAnalysisSession:
    """Reusable analysis artifacts for one Python file snapshot."""

    file_path: Path
    source: str
    source_bytes: bytes
    content_hash: str
    classifier_cache: ClassifierCacheContext = field(default_factory=ClassifierCacheContext)
    sg_root: SgRoot | None = None
    node_index: Any | None = None
    ast_tree: ast.Module | None = None
    symtable_table: symtable.SymbolTable | None = None
    compiled_module: CodeType | None = None
    tree_sitter_tree: Any | None = None
    resolution_index: dict[str, object] | None = None
    ast_span_index: tuple[AstSpanEntry, ...] | None = None
    stage_timings_ms: dict[str, float] = field(default_factory=dict)
    stage_errors: dict[str, str] = field(default_factory=dict)

    def _mark_stage(self, stage: str, started: float) -> None:
        self.stage_timings_ms[stage] = self.stage_timings_ms.get(stage, 0.0) + (
            (perf_counter() - started) * 1000.0
        )

    def ensure_sg_root(self) -> SgRoot | None:
        """Build or return cached ast-grep root.

        Returns:
        -------
        SgRoot | None
            Cached or newly constructed ast-grep root.
        """
        if self.sg_root is not None:
            return self.sg_root
        started = perf_counter()
        try:
            from tools.cq.search.pipeline.classifier import get_sg_root

            self.sg_root = get_sg_root(
                self.file_path,
                lang="python",
                cache_context=self.classifier_cache,
            )
        except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
            self.stage_errors["ast_grep"] = type(exc).__name__
            self.sg_root = None
        finally:
            self._mark_stage("ast_grep", started)
        return self.sg_root

    def ensure_node_index(self) -> Any | None:
        """Build or return cached ast-grep interval index.

        Returns:
        -------
        Any | None
            Cached or newly constructed interval index.
        """
        if self.node_index is not None:
            return self.node_index
        sg_root = self.ensure_sg_root()
        if sg_root is None:
            return None
        started = perf_counter()
        try:
            from tools.cq.search.pipeline.classifier import get_node_index

            self.node_index = get_node_index(
                self.file_path,
                sg_root,
                lang="python",
                cache_context=self.classifier_cache,
            )
        except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
            self.stage_errors["ast_grep_index"] = type(exc).__name__
            self.node_index = None
        finally:
            self._mark_stage("ast_grep_index", started)
        return self.node_index

    def ensure_ast(self) -> ast.Module | None:
        """Build or return cached Python ``ast`` tree.

        Returns:
        -------
        ast.Module | None
            Cached or newly parsed AST module.
        """
        if self.ast_tree is not None:
            return self.ast_tree
        started = perf_counter()
        try:
            self.ast_tree = ast.parse(self.source_bytes)
        except SyntaxError as exc:
            self.stage_errors["python_ast"] = type(exc).__name__
            self.ast_tree = None
        finally:
            self._mark_stage("python_ast", started)
        return self.ast_tree

    def ensure_symtable(self) -> symtable.SymbolTable | None:
        """Build or return cached symtable.

        Returns:
        -------
        symtable.SymbolTable | None
            Cached or newly constructed symbol table.
        """
        if self.symtable_table is not None:
            return self.symtable_table
        started = perf_counter()
        try:
            self.symtable_table = symtable.symtable(
                self.source,
                str(self.file_path),
                "exec",
            )
        except (RuntimeError, TypeError, ValueError, SyntaxError) as exc:
            self.stage_errors["symtable"] = type(exc).__name__
            self.symtable_table = None
        finally:
            self._mark_stage("symtable", started)
        return self.symtable_table

    def ensure_compiled_module(self) -> CodeType | None:
        """Build or return cached compiled module code object.

        Returns:
        -------
        CodeType | None
            Cached compiled module code object when compilation succeeds.
        """
        if self.compiled_module is not None:
            return self.compiled_module
        started = perf_counter()
        try:
            self.compiled_module = compile(self.source, str(self.file_path), "exec")
        except (SyntaxError, ValueError, TypeError) as exc:
            self.stage_errors["compile"] = type(exc).__name__
            self.compiled_module = None
        finally:
            self._mark_stage("compile", started)
        return self.compiled_module

    def ensure_resolution_index(self) -> dict[str, object] | None:
        """Return cached native resolution index payload.

        Returns:
        -------
        dict[str, object] | None
            Cached resolution index, if available.
        """
        return self.resolution_index

    def ensure_ast_span_index(self) -> tuple[AstSpanEntry, ...] | None:
        """Build or return cached AST byte-span index.

        Returns:
        -------
        tuple[AstSpanEntry, ...] | None
            Cached or newly built byte-span index.
        """
        if self.ast_span_index is not None:
            return self.ast_span_index
        tree = self.ensure_ast()
        if tree is None:
            return None
        started = perf_counter()
        rows: list[AstSpanEntry] = []
        try:
            for node, parents in _iter_nodes_with_parents(tree):
                span = node_byte_span(node, self.source_bytes)
                if span is None:
                    continue
                span_start, span_end = span
                rows.append(
                    AstSpanEntry(
                        node=node,
                        parents=parents,
                        byte_start=span_start,
                        byte_end=span_end,
                        priority=ast_node_priority(node),
                    )
                )
        except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
            self.stage_errors["ast_span_index"] = type(exc).__name__
            self.ast_span_index = None
        else:
            rows.sort(key=lambda item: (item.byte_start, item.byte_end))
            self.ast_span_index = tuple(rows)
        finally:
            self._mark_stage("ast_span_index", started)
        return self.ast_span_index

    def ensure_tree_sitter_tree(self) -> Any | None:
        """Build or return cached tree-sitter Python tree.

        Returns:
        -------
        Any | None
            Cached or newly parsed tree-sitter syntax tree.
        """
        if self.tree_sitter_tree is not None:
            return self.tree_sitter_tree
        started = perf_counter()
        try:
            from tools.cq.search.tree_sitter.python_lane.runtime import parse_python_tree

            self.tree_sitter_tree = parse_python_tree(self.source, cache_key=str(self.file_path))
        except (RuntimeError, TypeError, ValueError, AttributeError, ImportError) as exc:
            self.stage_errors["tree_sitter"] = type(exc).__name__
            self.tree_sitter_tree = None
        finally:
            self._mark_stage("tree_sitter", started)
        return self.tree_sitter_tree


def get_python_analysis_session(
    file_path: Path,
    source: str,
    *,
    sg_root: SgRoot | None = None,
) -> PythonAnalysisSession:
    """Get or create a per-file analysis session keyed by content hash.

    Returns:
    -------
    PythonAnalysisSession
        Reused or newly created analysis session for the file snapshot.
    """
    source_bytes = source.encode("utf-8", errors="replace")
    content_hash = source_hash(source_bytes)
    cache_key = str(file_path)
    cached = _SESSION_CACHE.get(cache_key)
    if cached is not None and cached.content_hash == content_hash:
        if cached.sg_root is None and sg_root is not None:
            cached.sg_root = sg_root
        return cached

    session = PythonAnalysisSession(
        file_path=file_path,
        source=source,
        source_bytes=source_bytes,
        content_hash=content_hash,
        sg_root=sg_root,
    )
    _SESSION_CACHE.put(cache_key, session)
    return session


def clear_python_analysis_sessions() -> None:
    """Clear in-process analysis sessions."""
    _SESSION_CACHE.clear()


def python_analysis_session_cache_size() -> int:
    """Return the current in-process session cache size."""
    return len(_SESSION_CACHE)


__all__ = [
    "AstSpanEntry",
    "PythonAnalysisSession",
    "clear_python_analysis_sessions",
    "get_python_analysis_session",
    "python_analysis_session_cache_size",
]
