"""Routing helpers for semantic pipeline dispatch."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from semantics.view_kinds import CONSOLIDATED_KIND

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFrameBuilder
    from semantics.pipeline_build import _SemanticSpecContext
    from semantics.registry import SemanticSpecIndex


def _semantic_view_specs(
    *,
    ordered_specs: Sequence[SemanticSpecIndex],
    context: _SemanticSpecContext,
) -> list[tuple[str, DataFrameBuilder]]:
    """Resolve ordered semantic view specs to builder tuples.

    Returns:
    -------
    list[tuple[str, DataFrameBuilder]]
        Output-name and builder tuples in topological order.
    """
    view_specs: list[tuple[str, DataFrameBuilder]] = []
    for spec in ordered_specs:
        builder = _builder_for_semantic_spec(spec, context=context)
        view_specs.append((spec.name, builder))
    return view_specs


def _dispatch_from_registry(
    registry_factory: Callable[[_SemanticSpecContext], Mapping[str, DataFrameBuilder]],
    context_label: str,
    *,
    finalize: bool = False,
) -> Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]:
    """Build dispatch handlers from a name-keyed registry factory.

    Returns:
    -------
    Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]
        Dispatch function resolving builders from the registry.
    """

    def _handler(
        spec: SemanticSpecIndex,
        context: _SemanticSpecContext,
    ) -> DataFrameBuilder:
        from semantics.pipeline_build import _finalize_output_builder

        mapping = registry_factory(context)
        builder = mapping.get(spec.name)
        if builder is None:
            msg = f"Missing {context_label} builder for output {spec.name!r}."
            raise KeyError(msg)
        if finalize:
            return _finalize_output_builder(spec.name, builder)
        return builder

    return _handler


def _builder_for_semantic_spec(
    spec: SemanticSpecIndex,
    *,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    """Resolve builder for one semantic spec entry.

    Returns:
    -------
    DataFrameBuilder
        Builder function for the semantic spec.

    Raises:
        ValueError: If the semantic kind is unsupported.
    """
    from semantics.pipeline_build import _CONSOLIDATED_BUILDER_HANDLERS

    consolidated_kind = CONSOLIDATED_KIND.get(spec.kind)
    if consolidated_kind is None:
        msg = f"Unsupported semantic spec kind: {spec.kind!r}."
        raise ValueError(msg)
    handler = _CONSOLIDATED_BUILDER_HANDLERS.get(consolidated_kind)
    if handler is None:
        msg = f"Unsupported consolidated semantic spec kind: {consolidated_kind!r}."
        raise ValueError(msg)
    return handler(spec, context)


def semantic_view_specs(
    *,
    ordered_specs: Sequence[SemanticSpecIndex],
    context: _SemanticSpecContext,
) -> list[tuple[str, DataFrameBuilder]]:
    """Public wrapper for semantic view-spec builder resolution.

    Returns:
    -------
    list[tuple[str, DataFrameBuilder]]
        Resolved output/builder tuples.
    """
    return _semantic_view_specs(ordered_specs=ordered_specs, context=context)


def dispatch_from_registry(
    registry_factory: Callable[[_SemanticSpecContext], Mapping[str, DataFrameBuilder]],
    context_label: str,
    *,
    finalize: bool = False,
) -> Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]:
    """Public wrapper for dispatch-factory construction.

    Returns:
    -------
    Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]
        Registry-backed dispatch handler.
    """
    return _dispatch_from_registry(registry_factory, context_label, finalize=finalize)


def builder_for_semantic_spec(
    spec: SemanticSpecIndex,
    *,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    """Public wrapper for one semantic-spec builder lookup.

    Returns:
    -------
    DataFrameBuilder
        Builder for the semantic spec.
    """
    return _builder_for_semantic_spec(spec, context=context)


__all__ = [
    "_builder_for_semantic_spec",
    "_dispatch_from_registry",
    "_semantic_view_specs",
    "builder_for_semantic_spec",
    "dispatch_from_registry",
    "semantic_view_specs",
]
