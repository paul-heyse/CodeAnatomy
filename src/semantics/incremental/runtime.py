"""Shared runtime helpers for incremental pipeline execution."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

import pyarrow as pa

from core_types import DeterminismTier
from datafusion_engine.catalog.introspection import invalidate_introspection_cache
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    PolicyBundleConfig,
    SessionRuntime,
)
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.registry_facade import RegistryFacade
    from semantics.program_manifest import ManifestDatasetResolver


@dataclass(frozen=True)
class IncrementalRuntimeBuildRequest:
    """Typed request for constructing an IncrementalRuntime."""

    dataset_resolver: ManifestDatasetResolver
    profile: DataFusionRuntimeProfile | None = None
    profile_name: str = "default"
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT


@dataclass
class IncrementalRuntime:
    """Runtime container for DataFusion incremental execution."""

    profile: DataFusionRuntimeProfile
    _session_runtime: SessionRuntime
    _dataset_resolver: ManifestDatasetResolver
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    _registry_facade_cache: RegistryFacade | None = field(default=None, init=False, repr=False)

    @classmethod
    def build(
        cls,
        request: IncrementalRuntimeBuildRequest,
    ) -> IncrementalRuntime:
        """Create a runtime with default DataFusion profile.

        Returns:
        -------
        IncrementalRuntime
            Newly constructed runtime instance.
        """
        runtime_profile = request.profile or DataFusionRuntimeProfile(
            policies=PolicyBundleConfig(config_policy_name=request.profile_name),
        )
        return cls(
            profile=runtime_profile,
            _session_runtime=runtime_profile.session_runtime(),
            _dataset_resolver=request.dataset_resolver,
            determinism_tier=request.determinism_tier,
        )

    @property
    def dataset_resolver(self) -> ManifestDatasetResolver:
        """Return the manifest-backed dataset resolver.

        Returns:
        -------
        ManifestDatasetResolver
            Dataset resolver from manifest bindings.
        """
        return self._dataset_resolver

    @property
    def registry_facade(self) -> RegistryFacade:
        """Return a RegistryFacade bound to this runtime and resolver."""
        if self._registry_facade_cache is not None:
            return self._registry_facade_cache
        from datafusion_engine.catalog.provider_registry import ProviderRegistry
        from datafusion_engine.dataset.registry import DatasetCatalog
        from datafusion_engine.registry_facade import RegistryFacade
        from datafusion_engine.udf.metadata import UdfCatalogAdapter

        dataset_catalog = DatasetCatalog()
        for name in self._dataset_resolver.names():
            location = self._dataset_resolver.location(name)
            if location is None:
                continue
            dataset_catalog.register(name, location, overwrite=True)
        provider_registry = ProviderRegistry(
            ctx=self._session_runtime.ctx,
            runtime_profile=self.profile,
        )
        try:
            udf_catalog = self.profile.udf_catalog(self._session_runtime.ctx)
        except (ValueError, RuntimeError, TypeError):
            udf_registry = None
        else:
            udf_registry = UdfCatalogAdapter(
                udf_catalog,
                function_factory_hash=self.profile.function_factory_policy_hash(
                    self._session_runtime.ctx
                ),
            )
        self._registry_facade_cache = RegistryFacade(
            dataset_catalog=dataset_catalog,
            provider_registry=provider_registry,
            udf_registry=udf_registry,
            view_registry=self.profile.view_registry,
        )
        return self._registry_facade_cache

    def session_runtime(self) -> SessionRuntime:
        """Return the cached DataFusion SessionRuntime.

        Returns:
        -------
        SessionRuntime
            Cached DataFusion session runtime.
        """
        return self._session_runtime

    def session_context(self) -> SessionContext:
        """Return the DataFusion SessionContext for compatibility.

        Returns:
        -------
        SessionContext
            Session context bound to the incremental SessionRuntime.
        """
        return self._session_runtime.ctx

    def io_adapter(self) -> DataFusionIOAdapter:
        """Return a DataFusion IO adapter bound to this runtime.

        Returns:
        -------
        DataFusionIOAdapter
            IO adapter bound to the runtime session.
        """
        return DataFusionIOAdapter(ctx=self._session_runtime.ctx, profile=self.profile)


class TempTableRegistry:
    """Track and cleanup temporary DataFusion tables."""

    def __init__(self, runtime: IncrementalRuntime) -> None:
        """__init__."""
        self._runtime = runtime
        self._ctx = runtime.session_runtime().ctx
        self._names: list[str] = []

    def register_table(self, table: pa.Table, *, prefix: str) -> str:
        """Register an Arrow table and track its name.

        Returns:
        -------
        str
            Registered table name.
        """
        name = f"__incremental_{prefix}_{uuid7_hex()}"
        self._runtime.io_adapter().register_arrow_table(name, table, overwrite=True)
        self._names.append(name)
        return name

    def track(self, name: str) -> str:
        """Track a table name registered elsewhere.

        Returns:
        -------
        str
            Tracked table name.
        """
        if name not in self._names:
            self._names.append(name)
        return name

    def deregister(self, name: str) -> None:
        """Deregister a tracked table name if possible."""
        deregister = getattr(self._ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(name)
                invalidate_introspection_cache(self._ctx)
        if name in self._names:
            self._names.remove(name)

    def close(self) -> None:
        """Deregister all tracked tables."""
        for name in list(self._names):
            self.deregister(name)

    def __enter__(self) -> Self:
        """Enter the context and return self.

        Returns:
        -------
        TempTableRegistry
            Context manager instance.
        """
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Exit the context and cleanup tracked tables."""
        self.close()


__all__ = ["IncrementalRuntime", "IncrementalRuntimeBuildRequest", "TempTableRegistry"]
