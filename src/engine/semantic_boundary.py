"""Guardrails to keep engine execution-only."""

from __future__ import annotations

from collections.abc import Sequence

from datafusion import SessionContext


def ensure_semantic_views_registered(
    ctx: SessionContext,
    view_names: Sequence[str] | None = None,
) -> None:
    """Raise if required semantic views are not registered.

    This function enforces the boundary between semantic definition (which
    owns view registration) and engine execution (which only consumes views).
    When engine attempts to materialize a semantic view, this guardrail
    ensures the view was properly registered by the semantic layer.

    Parameters
    ----------
    ctx
        DataFusion session context.
    view_names
        Optional list of view names to check. Defaults to SEMANTIC_VIEW_NAMES.

    Raises
    ------
    ValueError
        If any required semantic views are missing from the session.
    """
    from semantics.naming import SEMANTIC_VIEW_NAMES

    names = tuple(view_names) if view_names is not None else SEMANTIC_VIEW_NAMES
    missing = [name for name in names if not ctx.table_exist(name)]
    if missing:
        msg = f"Semantic views missing from session: {sorted(missing)}"
        raise ValueError(msg)


def is_semantic_view(view_name: str) -> bool:
    """Return whether a view name is a known semantic view.

    Parameters
    ----------
    view_name
        View name to check.

    Returns
    -------
    bool
        True if the view is a semantic view managed by the semantics layer.
    """
    from semantics.naming import SEMANTIC_VIEW_NAMES

    return view_name in SEMANTIC_VIEW_NAMES


__all__ = ["ensure_semantic_views_registered", "is_semantic_view"]
