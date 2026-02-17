"""CDF-aware input change resolution for semantic pipeline output scoping."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Protocol, cast

from datafusion_engine.dataset.registry import DatasetLocation

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.ir import SemanticIRView
    from semantics.program_manifest import ManifestDatasetResolver
    from semantics.registry import SemanticModel


class CdfCursorLike(Protocol):
    """Minimal cursor contract needed for CDF change detection."""

    last_version: int


class CdfCursorStoreLike(Protocol):
    """Minimal cursor-store contract needed for CDF change detection."""

    def load_cursor(self, name: str) -> CdfCursorLike | None:
        """Return the persisted cursor for ``name``, if present."""
        ...


class DeltaServiceLike(Protocol):
    """Minimal Delta service contract needed for CDF change detection."""

    def cdf_enabled(
        self,
        path: str,
        *,
        storage_options: Mapping[str, str],
        log_storage_options: Mapping[str, str],
    ) -> bool:
        """Return whether CDF is enabled for the Delta table at ``path``."""
        ...

    def table_version(
        self,
        *,
        path: str,
        storage_options: Mapping[str, str],
        log_storage_options: Mapping[str, str],
    ) -> int | None:
        """Return latest Delta table version for ``path`` when available."""
        ...


def cdf_changed_inputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    input_mapping: Mapping[str, str],
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> set[str] | None:
    """Return canonical semantic inputs with new CDF versions, or ``None`` if unavailable.

    Raises:
        ValueError: If ``dataset_resolver`` is not provided.
    """
    from semantics.incremental.cdf_cursors import CdfCursorStore

    _ = ctx
    if dataset_resolver is None:
        msg = "dataset_resolver is required for cdf_changed_inputs."
        raise ValueError(msg)
    cursor_store = runtime_profile.cdf_cursor_store()
    if cursor_store is None:
        return None
    if not isinstance(cursor_store, CdfCursorStore):
        return None
    typed_cursor_store = cast("CdfCursorStoreLike", cursor_store)
    delta_service = cast("DeltaServiceLike", runtime_profile.delta_ops.delta_service())
    return {
        canonical
        for canonical, source in input_mapping.items()
        if _input_has_cdf_changes(
            canonical=canonical,
            source=source,
            dataset_resolver=dataset_resolver,
            cursor_store=typed_cursor_store,
            delta_service=delta_service,
        )
    }


def resolve_cdf_location(
    *,
    canonical: str,
    source: str,
    dataset_resolver: ManifestDatasetResolver,
) -> DatasetLocation | None:
    """Resolve dataset location for a canonical/source input pair.

    Returns:
        DatasetLocation | None: Resolved Delta location or ``None`` when non-CDF input.
    """
    if canonical == "file_line_index_v1":
        return None
    location = dataset_resolver.location(canonical)
    if location is not None:
        return location
    return dataset_resolver.location(source)


def _input_has_cdf_changes(
    *,
    canonical: str,
    source: str,
    dataset_resolver: ManifestDatasetResolver,
    cursor_store: CdfCursorStoreLike,
    delta_service: DeltaServiceLike,
) -> bool:
    location = resolve_cdf_location(
        canonical=canonical,
        source=source,
        dataset_resolver=dataset_resolver,
    )
    if location is None:
        return False
    storage_options = dict(location.storage_options)
    log_options = dict(location.delta_log_storage_options)
    if not _cdf_enabled_for_location(
        location,
        storage_options=storage_options,
        log_storage_options=log_options,
        cdf_enabled_fn=delta_service.cdf_enabled,
    ):
        return False
    latest_version = delta_service.table_version(
        path=str(location.path),
        storage_options=storage_options,
        log_storage_options=log_options,
    )
    if latest_version is None:
        return False
    cursor = cursor_store.load_cursor(canonical)
    return cursor is None or latest_version > cursor.last_version


def _cdf_enabled_for_location(
    location: DatasetLocation,
    *,
    storage_options: Mapping[str, str],
    log_storage_options: Mapping[str, str],
    cdf_enabled_fn: Callable[..., bool],
) -> bool:
    if location.delta_cdf_options is not None:
        return True
    if location.datafusion_provider == "delta_cdf":
        return True
    return cdf_enabled_fn(
        str(location.path),
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )


def outputs_from_changed_inputs(
    changed_inputs: Collection[str],
    *,
    model: SemanticModel,
) -> set[str]:
    """Return output views impacted by changed semantic inputs."""
    from semantics.ir_pipeline import compile_semantics

    compiled = compile_semantics(model)
    impacted_views = _views_downstream_of_inputs(compiled.views, changed_inputs)
    output_names = {spec.name for spec in model.outputs}
    return {name for name in impacted_views if name in output_names}


def resolve_registered_cdf_inputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    use_cdf: bool | None,
    cdf_inputs: Mapping[str, str] | None,
    inputs: Sequence[str],
    dataset_resolver: ManifestDatasetResolver,
) -> Mapping[str, str] | None:
    """Resolve CDF input mapping, registering inputs when required.

    Returns:
        Mapping[str, str] | None: Merged CDF input mapping or ``None`` when CDF
            registration is not active.

    Raises:
        ValueError: If CDF is explicitly enabled but no runtime profile is
            provided.
    """
    if use_cdf is False:
        return None
    if runtime_profile is None:
        if use_cdf:
            msg = "CDF input registration requires a runtime profile."
            raise ValueError(msg)
        return dict(cdf_inputs) if cdf_inputs else None
    if use_cdf is None and not has_cdf_inputs(inputs=inputs, dataset_resolver=dataset_resolver):
        return dict(cdf_inputs) if cdf_inputs else None
    from datafusion_engine.session.introspection import register_cdf_inputs_for_profile

    mapping = register_cdf_inputs_for_profile(
        runtime_profile,
        ctx,
        table_names=inputs,
        dataset_resolver=dataset_resolver,
    )
    if cdf_inputs:
        mapping = {**mapping, **cdf_inputs}
    return mapping or None


def has_cdf_inputs(
    *,
    inputs: Sequence[str],
    dataset_resolver: ManifestDatasetResolver,
) -> bool:
    """Return whether any inputs resolve to CDF-enabled Delta sources."""
    from datafusion_engine.dataset.registry import resolve_datafusion_provider

    for name in inputs:
        location = dataset_resolver.location(name)
        if location is None:
            continue
        if location.delta_cdf_options is not None:
            return True
        if resolve_datafusion_provider(location) == "delta_cdf":
            return True
    return False


def _views_downstream_of_inputs(
    views: Sequence[SemanticIRView],
    seeds: Collection[str],
) -> set[str]:
    dependents: dict[str, list[str]] = {}
    for view in views:
        for dep in view.inputs:
            dependents.setdefault(dep, []).append(view.name)
    impacted: set[str] = set()
    stack = list(seeds)
    while stack:
        current = stack.pop()
        for view_name in dependents.get(current, ()):  # pragma: no branch - tiny loop
            if view_name in impacted:
                continue
            impacted.add(view_name)
            stack.append(view_name)
    return impacted


__all__ = [
    "CdfCursorLike",
    "CdfCursorStoreLike",
    "DeltaServiceLike",
    "cdf_changed_inputs",
    "has_cdf_inputs",
    "outputs_from_changed_inputs",
    "resolve_cdf_location",
    "resolve_registered_cdf_inputs",
]
