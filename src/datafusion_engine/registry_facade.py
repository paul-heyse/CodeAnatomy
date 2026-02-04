"""Capability-aware registry facade for unified registration flows."""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from utils.registry_protocol import Registry, SnapshotRegistry

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.catalog.provider_registry import ProviderRegistry
    from datafusion_engine.dataset.registration import DataFusionCachePolicy
    from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, DataFusionViewRegistry
    from datafusion_engine.udf.catalog import DataFusionUdfSpec, UdfCatalogAdapter


@dataclass(frozen=True)
class RegistrationResult:
    """Result of a registry registration operation."""

    success: bool
    key: str
    phase: str
    timestamp: float
    error: str | None = None


class RegistryFacade:
    """Unified registry facade with optional checkpoint/rollback semantics."""

    def __init__(
        self,
        *,
        dataset_catalog: DatasetCatalog,
        provider_registry: ProviderRegistry,
        udf_registry: UdfCatalogAdapter | None = None,
        view_registry: DataFusionViewRegistry | None = None,
    ) -> None:
        self._datasets = dataset_catalog
        self._providers = provider_registry
        self._udfs = udf_registry
        self._views = view_registry
        self._checkpoints: dict[str, dict[str, Mapping[object, object]]] = {}

    def register_dataset_df(
        self,
        *,
        name: str,
        location: DatasetLocation,
        cache_policy: DataFusionCachePolicy | None = None,
        overwrite: bool = False,
    ) -> DataFrame:
        """Register a dataset and return the DataFrame.

        Returns
        -------
        DataFrame
            DataFrame for the registered dataset.
        """
        checkpoint = self._checkpoint_optional()
        try:
            self._datasets.register(name, location, overwrite=overwrite)
            df = self._providers.register_location(
                name=name,
                location=location,
                overwrite=overwrite,
                cache_policy=cache_policy,
            )
            return df
        except Exception:
            self._rollback_optional(checkpoint)
            raise

    def register_dataset(
        self,
        *,
        name: str,
        location: DatasetLocation,
        cache_policy: DataFusionCachePolicy | None = None,
        overwrite: bool = False,
    ) -> RegistrationResult:
        """Register a dataset and return a result payload.

        Returns
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
        except Exception as exc:
            return RegistrationResult(
                success=False,
                key=name,
                phase="dataset",
                timestamp=start,
                error=str(exc),
            )
        return RegistrationResult(
            success=True,
            key=name,
            phase="dataset",
            timestamp=start,
        )

    def register_udf(self, *, key: str, spec: DataFusionUdfSpec) -> RegistrationResult:
        """Register a custom UDF spec in the metadata registry.

        Returns
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
        try:
            self._udfs.register(key, spec)
        except Exception as exc:
            return RegistrationResult(
                success=False,
                key=key,
                phase="udf",
                timestamp=start,
                error=str(exc),
            )
        return RegistrationResult(
            success=True,
            key=key,
            phase="udf",
            timestamp=start,
        )

    def register_view(self, *, name: str, artifact: object) -> RegistrationResult:
        """Register a view artifact when a view registry is available.

        Returns
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
        try:
            self._views.register(name, artifact)
        except Exception as exc:
            return RegistrationResult(
                success=False,
                key=name,
                phase="view",
                timestamp=start,
                error=str(exc),
            )
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
        checkpoint_id = str(uuid.uuid4())
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
            "datasets": self._datasets,
            "providers": self._providers,
        }
        if self._udfs is not None:
            registries["udfs"] = self._udfs
        if self._views is not None:
            registries["views"] = self._views
        return registries


def registry_facade_for_context(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> RegistryFacade:
    """Return a RegistryFacade bound to a session context.

    Returns
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
        udf_registry = UdfCatalogAdapter(udf_catalog)
    view_registry = runtime_profile.view_registry
    return RegistryFacade(
        dataset_catalog=dataset_catalog,
        provider_registry=provider_registry,
        udf_registry=udf_registry,
        view_registry=view_registry,
    )


__all__ = ["RegistrationResult", "RegistryFacade", "registry_facade_for_context"]
