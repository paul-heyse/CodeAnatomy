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
        Optional list of view names to check. Defaults to semantic model outputs.

    Raises
    ------
    ValueError
        If any required semantic views are missing from the session.
    """
    from semantics.registry import SEMANTIC_MODEL

    names = (
        tuple(view_names)
        if view_names is not None
        else tuple(spec.name for spec in SEMANTIC_MODEL.outputs)
    )
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
    from semantics.registry import SEMANTIC_MODEL

    return view_name in {spec.name for spec in SEMANTIC_MODEL.outputs}


__all__ = ["ensure_semantic_views_registered", "is_semantic_view"]
