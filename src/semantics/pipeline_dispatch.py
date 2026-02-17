"""Routing helpers for semantic pipeline dispatch."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from semantics.view_kinds import CONSOLIDATED_KIND

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFrameBuilder
    from semantics.registry import SemanticSpecIndex


def _semantic_view_specs[ContextT](
    *,
    ordered_specs: Sequence[SemanticSpecIndex],
    context: ContextT,
    builder_for_semantic_spec: Callable[[SemanticSpecIndex, ContextT], DataFrameBuilder],
) -> list[tuple[str, DataFrameBuilder]]:
    """Resolve ordered semantic view specs to builder tuples.

    Returns:
    -------
    list[tuple[str, DataFrameBuilder]]
        Output-name and builder tuples in topological order.
    """
    view_specs: list[tuple[str, DataFrameBuilder]] = []
    for spec in ordered_specs:
        builder = builder_for_semantic_spec(spec, context)
        view_specs.append((spec.name, builder))
    return view_specs


def _dispatch_from_registry[ContextT](
    registry_factory: Callable[[ContextT], Mapping[str, DataFrameBuilder]],
    context_label: str,
    *,
    finalize_builder: Callable[[str, DataFrameBuilder], DataFrameBuilder] | None = None,
) -> Callable[[SemanticSpecIndex, ContextT], DataFrameBuilder]:
    """Build dispatch handlers from a name-keyed registry factory.

    Returns:
    -------
    Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]
        Dispatch function resolving builders from the registry.
    """

    def _handler(
        spec: SemanticSpecIndex,
        context: ContextT,
    ) -> DataFrameBuilder:
        mapping = registry_factory(context)
        builder = mapping.get(spec.name)
        if builder is None:
            msg = f"Missing {context_label} builder for output {spec.name!r}."
            raise KeyError(msg)
        if finalize_builder is not None:
            return finalize_builder(spec.name, builder)
        return builder

    return _handler


def _builder_for_semantic_spec[ContextT](
    spec: SemanticSpecIndex,
    *,
    context: ContextT,
    builder_handlers: Mapping[str, Callable[[SemanticSpecIndex, ContextT], DataFrameBuilder]],
) -> DataFrameBuilder:
    """Resolve builder for one semantic spec entry.

    Returns:
    -------
    DataFrameBuilder
        Builder function for the semantic spec.

    Raises:
        ValueError: If the semantic kind is unsupported.
    """
    consolidated_kind = CONSOLIDATED_KIND.get(spec.kind)
    if consolidated_kind is None:
        msg = f"Unsupported semantic spec kind: {spec.kind!r}."
        raise ValueError(msg)
    handler = builder_handlers.get(consolidated_kind)
    if handler is None:
        msg = f"Unsupported consolidated semantic spec kind: {consolidated_kind!r}."
        raise ValueError(msg)
    return handler(spec, context)


def semantic_view_specs[ContextT](
    *,
    ordered_specs: Sequence[SemanticSpecIndex],
    context: ContextT,
    builder_for_semantic_spec: Callable[[SemanticSpecIndex, ContextT], DataFrameBuilder],
) -> list[tuple[str, DataFrameBuilder]]:
    """Public wrapper for semantic view-spec builder resolution.

    Returns:
    -------
    list[tuple[str, DataFrameBuilder]]
        Resolved output/builder tuples.
    """
    return _semantic_view_specs(
        ordered_specs=ordered_specs,
        context=context,
        builder_for_semantic_spec=builder_for_semantic_spec,
    )


def dispatch_from_registry[ContextT](
    registry_factory: Callable[[ContextT], Mapping[str, DataFrameBuilder]],
    context_label: str,
    *,
    finalize_builder: Callable[[str, DataFrameBuilder], DataFrameBuilder] | None = None,
) -> Callable[[SemanticSpecIndex, ContextT], DataFrameBuilder]:
    """Public wrapper for dispatch-factory construction.

    Returns:
    -------
    Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]
        Registry-backed dispatch handler.
    """
    return _dispatch_from_registry(
        registry_factory,
        context_label,
        finalize_builder=finalize_builder,
    )


def builder_for_semantic_spec[ContextT](
    spec: SemanticSpecIndex,
    *,
    context: ContextT,
    builder_handlers: Mapping[str, Callable[[SemanticSpecIndex, ContextT], DataFrameBuilder]],
) -> DataFrameBuilder:
    """Public wrapper for one semantic-spec builder lookup.

    Returns:
    -------
    DataFrameBuilder
        Builder for the semantic spec.
    """
    return _builder_for_semantic_spec(
        spec,
        context=context,
        builder_handlers=builder_handlers,
    )


__all__ = [
    "_builder_for_semantic_spec",
    "_dispatch_from_registry",
    "_semantic_view_specs",
    "builder_for_semantic_spec",
    "dispatch_from_registry",
    "semantic_view_specs",
]
