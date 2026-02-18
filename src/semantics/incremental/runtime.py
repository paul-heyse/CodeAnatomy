"""Shared runtime helpers for incremental pipeline execution."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

import pyarrow as pa

from core_types import DeterminismTier
from datafusion_engine.catalog.introspection import invalidate_introspection_cache
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.session.profiles import create_runtime_profile
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_session import SessionRuntime
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta.service_protocol import DeltaServicePort
    from datafusion_engine.delta.store_policy import DeltaStorePolicy
    from datafusion_engine.lineage.diagnostics import DiagnosticsSink
    from datafusion_engine.registry_facade import RegistryFacade
    from schema_spec.scan_policy import ScanPolicyConfig
    from semantics.program_manifest import ManifestDatasetResolver


@dataclass(frozen=True)
class IncrementalRuntimeBuildRequest:
    """Typed request for constructing an IncrementalRuntime."""

    dataset_resolver: ManifestDatasetResolver
    profile: DataFusionRuntimeProfile | None = None
    delta_service: DeltaServicePort | None = None
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

        Raises:
            ValueError: If neither an explicit profile nor an explicit delta service
                is provided.
        """
        if request.profile is None and request.delta_service is None:
            msg = (
                "IncrementalRuntime.build requires an explicit runtime profile or "
                "an explicit delta service binding."
            )
            raise ValueError(msg)
        runtime_profile = request.profile or create_runtime_profile(
            config_policy_name=request.profile_name,
        )
        if request.delta_service is not None:
            from datafusion_engine.session.runtime_ops import bind_delta_service

            bind_delta_service(runtime_profile, service=request.delta_service)
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

    def delta_service(self) -> DeltaServicePort:
        """Return the Delta service from the runtime profile."""
        return self.profile.delta_ops.delta_service()

    def scan_policy(self) -> ScanPolicyConfig:
        """Return the scan policy from the runtime profile."""
        return self.profile.policies.scan_policy

    def delta_store_policy(self) -> DeltaStorePolicy | None:
        """Return the Delta store policy from the runtime profile."""
        return self.profile.policies.delta_store_policy

    def diagnostics_sink(self) -> DiagnosticsSink | None:
        """Return the diagnostics sink, if configured."""
        return self.profile.diagnostics.diagnostics_sink

    def settings_hash(self) -> str:
        """Return the DataFusion settings hash for reproducibility."""
        return self.profile.settings_hash()

    def config_policy_name(self) -> str | None:
        """Return the runtime policy-profile name."""
        return self.profile.policies.config_policy_name


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
