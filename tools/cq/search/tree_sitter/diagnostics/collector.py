"""Tree-sitter diagnostics extraction (`ERROR`/`MISSING`) helpers."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
    TreeSitterDiagnosticV1,
)
from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_matches
from tools.cq.search.tree_sitter.diagnostics.recovery_hints import recovery_hints_for_node

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Query

try:
    import tree_sitter_python as _tree_sitter_python
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_python = None

try:
    import tree_sitter_rust as _tree_sitter_rust
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_rust = None

try:
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Query as _TreeSitterQuery
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterLanguage = None
    _TreeSitterQuery = None


@lru_cache(maxsize=2)
def _language(language: str) -> Language:
    if _TreeSitterLanguage is None:
        msg = "tree_sitter language bindings are unavailable"
        raise RuntimeError(msg)
    if language == "python":
        if _tree_sitter_python is None:
            msg = "tree_sitter_python bindings are unavailable"
            raise RuntimeError(msg)
        return _TreeSitterLanguage(_tree_sitter_python.language())
    if _tree_sitter_rust is None:
        msg = "tree_sitter_rust bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterLanguage(_tree_sitter_rust.language())


@lru_cache(maxsize=2)
def _diagnostic_query(language: str) -> Query | None:
    if _TreeSitterQuery is None:
        return None
    path = Path(__file__).with_suffix("").parent / "queries" / language / "95_diagnostics.scm"
    if not path.exists():
        return None
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return _TreeSitterQuery(_language(language), source)
    except (RuntimeError, TypeError, ValueError):
        return None


def collect_tree_sitter_diagnostics(
    *,
    language: str,
    root: Node,
    windows: tuple[QueryWindowV1, ...],
    match_limit: int = 1024,
) -> tuple[TreeSitterDiagnosticV1, ...]:
    """Collect diagnostics rows for one tree/window set.

    Returns:
        tuple[TreeSitterDiagnosticV1, ...]: A tuple of diagnostic rows.
    """
    try:
        language_obj = _language(language)
    except (RuntimeError, TypeError, ValueError):
        return ()
    query = _diagnostic_query(language)
    if query is None:
        return ()
    matches, _telemetry = run_bounded_query_matches(
        query,
        root,
        windows=windows,
        settings=QueryExecutionSettingsV1(
            match_limit=match_limit,
            require_containment=True,
        ),
    )
    rows: list[TreeSitterDiagnosticV1] = []
    for _idx, capture_map in matches:
        for capture_name, nodes in capture_map.items():
            if not isinstance(capture_name, str):
                continue
            kind = "ERROR" if capture_name.endswith("error") else "MISSING"
            for node in nodes:
                start_point = getattr(node, "start_point", (0, 0))
                end_point = getattr(node, "end_point", start_point)
                rows.append(
                    TreeSitterDiagnosticV1(
                        kind=kind,
                        start_byte=int(getattr(node, "start_byte", 0)),
                        end_byte=int(getattr(node, "end_byte", 0)),
                        start_line=int(start_point[0]) + 1,
                        start_col=int(start_point[1]),
                        end_line=int(end_point[0]) + 1,
                        end_col=int(end_point[1]),
                        message=f"tree-sitter {kind}",
                        metadata={
                            "capture": capture_name,
                            "expected": list(
                                recovery_hints_for_node(
                                    language=language_obj,
                                    node=node,
                                )
                            ),
                        },
                    )
                )
    return tuple(rows)


__all__ = ["collect_tree_sitter_diagnostics"]
