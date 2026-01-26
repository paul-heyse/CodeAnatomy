"""Hamilton nodes for view graph registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

from hamilton.function_modifiers import tag

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.view_registry import ensure_view_graph
from datafusion_engine.view_registry_specs import view_graph_nodes

if TYPE_CHECKING:
    from datafusion_engine.view_graph_registry import ViewNode


@tag(layer="views", artifact="view_nodes", kind="graph")
def view_nodes(ctx: ExecutionContext) -> tuple[ViewNode, ...]:
    """Return view graph nodes with SQLGlot ASTs attached.

    Returns
    -------
    tuple[ViewNode, ...]
        View nodes derived from registered views.

    Raises
    ------
    ValueError
        Raised when the DataFusion runtime profile is missing.
    """
    profile = ctx.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required for view graph nodes."
        raise ValueError(msg)
    session = profile.session_context()
    snapshot = ensure_view_graph(
        session,
        runtime_profile=profile,
        include_registry_views=True,
    )
    nodes = view_graph_nodes(session, snapshot=snapshot)
    return tuple(nodes)


__all__ = ["view_nodes"]
