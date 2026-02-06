"""Capability-aware registry facade for unified registration flows."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

from datafusion_engine.catalog.provider_registry import RegistrationMetadata
from datafusion_engine.errors import DataFusionEngineError
from serde_msgspec import StructBaseStrict
from utils.registry_protocol import Registry, SnapshotRegistry
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.dataset.registration import DataFusionCachePolicy
    from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, DataFusionViewRegistry
    from datafusion_engine.udf.catalog import DataFusionUdfSpec, UdfCatalogAdapter
    from datafusion_engine.views.artifacts import DataFusionViewArtifact


class ProviderRegistryLike(
    Registry[str, RegistrationMetadata], SnapshotRegistry[str, RegistrationMetadata], Protocol
):
    """Protocol for provider registries used by the facade."""

    def register_location(
        self,
        *,
        name: str,
        location: DatasetLocation,
        overwrite: bool = False,
        cache_policy: DataFusionCachePolicy | None = None,
    ) -> DataFrame: ...


class RegistrationResult(StructBaseStrict, frozen=True):
    """Result of a registry registration operation."""

    success: bool
    key: str
    phase: str
    timestamp: float
    error: str | None = None


_REGISTRATION_ERRORS = (DataFusionEngineError, KeyError, RuntimeError, TypeError, ValueError)


def _registration_failure(
    *,
    key: str,
    phase: str,
    start: float,
    exc: Exception,
) -> RegistrationResult:
    return RegistrationResult(
        success=False,
        key=key,
        phase=phase,
        timestamp=start,
        error=str(exc),
    )


@dataclass(frozen=True)
class RegistrationPhase:
    """Registration phase with ordering requirements and optional validation."""

    name: str
    requires: tuple[str, ...] = ()
    validate: Callable[[], None] | None = None


class RegistrationPhaseOrchestrator:
    """Enforce registration phase ordering and execute validations."""

    @staticmethod
    def run(phases: Sequence[RegistrationPhase]) -> None:
        """Run registration phases in order, enforcing prerequisites.

        Args:
            phases: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        seen: set[str] = set()
        for phase in phases:
            if phase.name in seen:
                msg = f"Duplicate registration phase: {phase.name!r}."
                raise ValueError(msg)
            missing = tuple(req for req in phase.requires if req not in seen)
            if missing:
                msg = f"Registration phase {phase.name!r} requires prior phases {missing!r}."
                raise ValueError(msg)
            if phase.validate is not None:
                phase.validate()
            seen.add(phase.name)


def _default_registry_adapters() -> dict[str, Registry[object, object]]:
    from datafusion_engine.schema.registry import (
        base_extract_schema_registry,
        nested_dataset_registry,
        relationship_schema_registry,
        root_identity_registry,
    )
    from semantics.registry import (
        semantic_normalization_registry,
        semantic_relationship_registry,
        semantic_table_registry,
    )

    return {
        "semantic_tables": cast("Registry[object, object]", semantic_table_registry()),
        "semantic_normalization": cast(
            "Registry[object, object]", semantic_normalization_registry()
        ),
        "semantic_relationships": cast(
            "Registry[object, object]", semantic_relationship_registry()
        ),
        "schema_relationships": cast("Registry[object, object]", relationship_schema_registry()),
        "schema_extract_base": cast("Registry[object, object]", base_extract_schema_registry()),
        "schema_nested": cast("Registry[object, object]", nested_dataset_registry()),
        "schema_root_identity": cast("Registry[object, object]", root_identity_registry()),
    }


class RegistryFacade:
    """Unified registry facade with optional checkpoint/rollback semantics."""

    def __init__(
        self,
        *,
        dataset_catalog: DatasetCatalog,
        provider_registry: ProviderRegistryLike,
        udf_registry: UdfCatalogAdapter | None = None,
        view_registry: DataFusionViewRegistry | None = None,
    ) -> None:
        """Initialize the instance.

        Args:
            dataset_catalog: Description.
            provider_registry: Description.
            udf_registry: Description.
            view_registry: Description.
        """
        self._datasets = dataset_catalog
        self._providers = provider_registry
        self._udfs = udf_registry
        self._views = view_registry
        self._checkpoints: dict[str, dict[str, Mapping[object, object]]] = {}
        self._extra_registries = _default_registry_adapters()

    def register_dataset_df(
        self,
        *,
        name: str,
        location: DatasetLocation,
        cache_policy: DataFusionCachePolicy | None = None,
        overwrite: bool = False,
    ) -> DataFrame:
        """Register a dataset and return the DataFrame.

        Args:
            name: Dataset name to register.
            location: Dataset location descriptor.
            cache_policy: Optional cache policy override.
            overwrite: Whether to overwrite an existing registration.

        Returns:
            DataFrame: Result.

        Raises:
            RuntimeError: If provider registration does not return a DataFrame.
        """
        checkpoint = self._checkpoint_optional()
        df: DataFrame | None = None

        def _register_dataset() -> None:
            self._datasets.register(name, location, overwrite=overwrite)

        def _register_provider() -> None:
            nonlocal df
            df = self._providers.register_location(
                name=name,
                location=location,
                overwrite=overwrite,
                cache_policy=cache_policy,
            )

        try:
            RegistrationPhaseOrchestrator().run(
                (
                    RegistrationPhase(name="dataset_catalog", validate=_register_dataset),
                    RegistrationPhase(
                        name="provider_registry",
                        requires=("dataset_catalog",),
                        validate=_register_provider,
                    ),
                )
            )
        except Exception:
            self._rollback_optional(checkpoint)
            raise
        if df is None:
            self._rollback_optional(checkpoint)
            msg = "Provider registry did not return a DataFrame."
            raise RuntimeError(msg)
        return df

    def register_dataset(
        self,
        *,
        name: str,
        location: DatasetLocation,
        cache_policy: DataFusionCachePolicy | None = None,
        overwrite: bool = False,
    ) -> RegistrationResult:
        """Register a dataset and return a result payload.

        Returns:
        -------
        RegistrationResult
            Registration outcome payload.
        """
        start = time.time()
        try:
            self.register_dataset_df(
                name=name,
                location=location,
                cache_policy=cache_policy,
                overwrite=overwrite,
            )
        except _REGISTRATION_ERRORS as exc:
            return _registration_failure(
                key=name,
                phase="dataset",
                start=start,
                exc=exc,
            )
        else:
            return RegistrationResult(
                success=True,
                key=name,
                phase="dataset",
                timestamp=start,
            )

    def register_udf(self, *, key: str, spec: DataFusionUdfSpec) -> RegistrationResult:
        """Register a custom UDF spec in the metadata registry.

        Returns:
        -------
        RegistrationResult
            Registration outcome payload.
        """
        start = time.time()
        if self._udfs is None:
            return RegistrationResult(
                success=False,
                key=key,
                phase="udf",
                timestamp=start,
                error="UDF registry unavailable.",
            )
        udf_registry = self._udfs
        assert udf_registry is not None

        def _register_udf() -> None:
            udf_registry.register(key, spec)

        try:
            RegistrationPhaseOrchestrator().run(
                (RegistrationPhase(name="udf_registry", validate=_register_udf),)
            )
        except _REGISTRATION_ERRORS as exc:
            return _registration_failure(
                key=key,
                phase="udf",
                start=start,
                exc=exc,
            )
        else:
            return RegistrationResult(
                success=True,
                key=key,
                phase="udf",
                timestamp=start,
            )

    def register_view(self, *, name: str, artifact: DataFusionViewArtifact) -> RegistrationResult:
        """Register a view artifact when a view registry is available.

        Returns:
        -------
        RegistrationResult
            Registration outcome payload.
        """
        start = time.time()
        if self._views is None:
            return RegistrationResult(
                success=False,
                key=name,
                phase="view",
                timestamp=start,
                error="View registry unavailable.",
            )
        view_registry = self._views
        assert view_registry is not None

        def _register_view() -> None:
            view_registry.register(name, artifact)

        try:
            RegistrationPhaseOrchestrator().run(
                (RegistrationPhase(name="view_registry", validate=_register_view),)
            )
        except _REGISTRATION_ERRORS as exc:
            return _registration_failure(
                key=name,
                phase="view",
                start=start,
                exc=exc,
            )
        else:
            return RegistrationResult(
                success=True,
                key=name,
                phase="view",
                timestamp=start,
            )

    def _checkpoint_optional(self) -> str | None:
        snapshot: dict[str, Mapping[object, object]] = {}
        for label, registry in self._registries().items():
            if isinstance(registry, SnapshotRegistry):
                snapshot[label] = registry.snapshot()
        if not snapshot:
            return None
        checkpoint_id = uuid7_str()
        self._checkpoints[checkpoint_id] = snapshot
        return checkpoint_id

    def _rollback_optional(self, checkpoint_id: str | None) -> None:
        if checkpoint_id is None:
            return
        snapshot = self._checkpoints.get(checkpoint_id)
        if snapshot is None:
            return
        for label, registry in self._registries().items():
            if isinstance(registry, SnapshotRegistry) and label in snapshot:
                registry.restore(snapshot[label])

    def _registries(self) -> Mapping[str, Registry[object, object]]:
        registries: dict[str, Registry[object, object]] = {
            "datasets": cast("Registry[object, object]", self._datasets),
            "providers": cast("Registry[object, object]", self._providers),
        }
        if self._udfs is not None:
            registries["udfs"] = cast("Registry[object, object]", self._udfs)
        if self._views is not None:
            registries["views"] = cast("Registry[object, object]", self._views)
        registries.update(self._extra_registries)
        return registries


def registry_facade_for_context(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> RegistryFacade:
    """Return a RegistryFacade bound to a session context.

    Returns:
    -------
    RegistryFacade
        Registry facade bound to the provided context.
    """
    from datafusion_engine.catalog.provider_registry import ProviderRegistry
    from datafusion_engine.dataset.registry import dataset_catalog_from_profile
    from datafusion_engine.udf.catalog import UdfCatalogAdapter

    dataset_catalog = dataset_catalog_from_profile(runtime_profile)
    provider_registry = ProviderRegistry(ctx=ctx, runtime_profile=runtime_profile)
    try:
        udf_catalog = runtime_profile.udf_catalog(ctx)
    except (ValueError, RuntimeError, TypeError):
        udf_registry = None
    else:
        udf_registry = UdfCatalogAdapter(
            udf_catalog,
            function_factory_hash=runtime_profile.function_factory_policy_hash(ctx),
        )
    view_registry = runtime_profile.view_registry
    return RegistryFacade(
        dataset_catalog=dataset_catalog,
        provider_registry=provider_registry,
        udf_registry=udf_registry,
        view_registry=view_registry,
    )


__all__ = [
    "RegistrationPhase",
    "RegistrationPhaseOrchestrator",
    "RegistrationResult",
    "RegistryFacade",
    "registry_facade_for_context",
]
