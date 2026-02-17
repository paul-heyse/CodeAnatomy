"""Runtime profile operational helpers: Delta ops, I/O, catalog, facade mixins."""

from __future__ import annotations

import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast
from weakref import WeakKeyDictionary

import msgspec

from datafusion_engine.extensions.context_adaptation import (
    resolve_extension_module as _resolve_extension_module_contract,
)
from datafusion_engine.registry_facade import RegistrationPhaseOrchestrator

if TYPE_CHECKING:
    from typing import Protocol

    from datafusion import SessionContext

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta.service_protocol import DeltaServicePort
    from obs.datafusion_runs import DataFusionRun
    from semantics.program_manifest import ManifestDatasetResolver
    from serde_schema_registry import ArtifactSpec
    from storage.deltalake.delta_read import IdempotentWriteOptions

    class _DeltaRuntimeEnvOptions(Protocol):
        max_spill_size: int | None
        max_temp_directory_size: int | None


_EXTENSION_MODULE_NAMES: tuple[str, ...] = ("datafusion_engine.extensions.datafusion_ext",)
_BOUND_DELTA_SERVICES: WeakKeyDictionary[object, object] = WeakKeyDictionary()


def bind_delta_service(
    profile: DataFusionRuntimeProfile,
    *,
    service: DeltaServicePort | None,
) -> None:
    """Bind an explicit Delta service implementation to a runtime profile."""
    if service is None:
        _BOUND_DELTA_SERVICES.pop(profile, None)
        return
    _BOUND_DELTA_SERVICES[profile] = service


def _resolve_delta_service(profile: DataFusionRuntimeProfile) -> DeltaServicePort:
    bound = _BOUND_DELTA_SERVICES.get(profile)
    if bound is not None:
        return cast("DeltaServicePort", bound)
    msg = (
        "DeltaServicePort is not bound to this runtime profile. "
        "Profiles must bind an explicit delta service during construction."
    )
    raise RuntimeError(msg)


def _resolve_runtime_extension_module(required_attr: str | None = None) -> object | None:
    resolved = _resolve_extension_module_contract(
        _EXTENSION_MODULE_NAMES,
        required_attr=required_attr,
    )
    if resolved is None:
        return None
    _module_name, module = resolved
    return module


def delta_runtime_env_options(
    profile: DataFusionRuntimeProfile,
) -> _DeltaRuntimeEnvOptions | None:
    """Return delta runtime env options for a profile.

    Parameters
    ----------
    profile
        Runtime profile with delta configuration.

    Returns:
    -------
    _DeltaRuntimeEnvOptions | None
        Delta runtime env options or None when not configured.

    Raises:
        RuntimeError: When datafusion_ext is unavailable.
        TypeError: When the options type is not callable.
    """
    if (
        profile.execution.delta_max_spill_size is None
        and profile.execution.delta_max_temp_directory_size is None
    ):
        return None
    module = _resolve_runtime_extension_module(required_attr="DeltaRuntimeEnvOptions")
    if module is None:
        msg = "Delta runtime env options require datafusion_ext."
        raise RuntimeError(msg)
    options_cls = getattr(module, "DeltaRuntimeEnvOptions", None)
    if not callable(options_cls):
        msg = "Delta runtime env options type is unavailable in the extension module."
        raise TypeError(msg)
    options = cast("_DeltaRuntimeEnvOptions", options_cls())
    if profile.execution.delta_max_spill_size is not None:
        options.max_spill_size = int(profile.execution.delta_max_spill_size)
    if profile.execution.delta_max_temp_directory_size is not None:
        options.max_temp_directory_size = int(profile.execution.delta_max_temp_directory_size)
    return options


@dataclass(frozen=True)
class RuntimeProfileDeltaOps:
    """Delta-specific runtime operations bound to a profile."""

    profile: DataFusionRuntimeProfile

    def delta_runtime_ctx(self) -> SessionContext:
        """Return a SessionContext configured for Delta operations.

        Returns:
        -------
        SessionContext
            Session context configured for Delta operations.
        """
        return self.profile.session_context()

    def delta_service(self) -> DeltaServicePort:
        """Return the Delta service bound to this runtime profile.

        Returns:
        -------
        DeltaServicePort
            Delta service bound to the runtime profile.
        """
        return _resolve_delta_service(self.profile)

    def reserve_delta_commit(
        self,
        *,
        key: str,
        metadata: Mapping[str, object] | None = None,
        commit_metadata: Mapping[str, str] | None = None,
    ) -> tuple[IdempotentWriteOptions, DataFusionRun]:
        """Reserve the next idempotent commit version for a Delta write.

        Returns:
        -------
        tuple[IdempotentWriteOptions, DataFusionRun]
            Commit options plus the updated run context.
        """
        run = self.profile.delta_commit_runs.get(key)
        if run is None:
            from obs.datafusion_runs import create_run_context

            base_metadata: dict[str, str] = {"key": key}
            if metadata:
                base_metadata.update(
                    {str(item_key): str(item_value) for item_key, item_value in metadata.items()}
                )
            run = create_run_context(
                label="delta_commit",
                sink=self.profile.diagnostics.diagnostics_sink,
                metadata=base_metadata,
            )
            self.profile.delta_commit_runs[key] = run
        elif metadata:
            run.metadata.update(dict(metadata))
        if commit_metadata:
            run.metadata["commit_metadata"] = dict(commit_metadata)
        options, updated = run.next_commit_version()
        if self.profile.diagnostics.diagnostics_sink is not None:
            commit_meta_payload = dict(commit_metadata) if commit_metadata is not None else None
            payload = {
                "event_time_unix_ms": int(time.time() * 1000),
                "key": key,
                "run_id": run.run_id,
                "app_id": options.app_id,
                "version": options.version,
                "commit_sequence": run.commit_sequence,
                "status": "reserved",
                "metadata": dict(run.metadata),
                "commit_metadata": commit_meta_payload,
            }
            self.profile.record_artifact(
                _delta_commit_spec(),
                payload,
            )
        return options, updated

    def finalize_delta_commit(
        self,
        *,
        key: str,
        run: DataFusionRun,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        """Persist commit sequencing state after a successful write."""
        if metadata:
            run.metadata.update(dict(metadata))
        self.profile.delta_commit_runs[key] = run
        if self.profile.diagnostics.diagnostics_sink is None:
            return
        committed_version = run.commit_sequence - 1 if run.commit_sequence > 0 else None
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "key": key,
            "run_id": run.run_id,
            "app_id": run.run_id,
            "version": committed_version,
            "commit_sequence": run.commit_sequence,
            "status": "finalized",
            "metadata": dict(run.metadata),
        }
        self.profile.record_artifact(
            _delta_commit_spec(),
            payload,
        )

    def ensure_delta_plan_codecs(self, ctx: SessionContext) -> bool:
        """Install Delta plan codecs when enabled.

        Returns:
        -------
        bool
            True when codecs are installed.
        """
        if not self.profile.features.enable_delta_plan_codecs:
            return False
        available, installed = self.profile.install_delta_plan_codecs(ctx)
        self.profile.record_delta_plan_codecs_event(
            available=available,
            installed=installed,
        )
        return installed


@dataclass(frozen=True)
class RuntimeProfileIO:
    """Runtime I/O helpers for cache and ephemeral contexts."""

    profile: DataFusionRuntimeProfile

    def cache_root(self) -> str:
        """Return the root directory for Delta-backed caches.

        Returns:
        -------
        str
            Cache root directory.
        """
        if self.profile.policies.cache_output_root is not None:
            return self.profile.policies.cache_output_root
        return str(Path(tempfile.gettempdir()) / "datafusion_cache")

    def runtime_artifact_root(self) -> str:
        """Return the root directory for runtime artifact cache tables.

        Returns:
        -------
        str
            Runtime artifact cache root.
        """
        if self.profile.policies.runtime_artifact_cache_root is not None:
            return self.profile.policies.runtime_artifact_cache_root
        return str(Path(self.cache_root()) / "runtime_artifacts")

    def metadata_cache_snapshot_root(self) -> str:
        """Return the root directory for metadata cache snapshots.

        Returns:
        -------
        str
            Metadata cache snapshot root.
        """
        return str(Path(self.cache_root()) / "metadata_cache_snapshots")

    def ephemeral_context(self) -> SessionContext:
        """Return a non-cached SessionContext configured from the profile.

        Returns:
        -------
        SessionContext
            Ephemeral session context configured for this profile.
        """
        ctx = self.profile.build_ephemeral_context()
        RegistrationPhaseOrchestrator().run(self.profile.ephemeral_context_phases(ctx))
        return ctx


@dataclass(frozen=True)
class RuntimeProfileCatalog:
    """Catalog helpers bound to a runtime profile."""

    profile: DataFusionRuntimeProfile

    def ast_dataset_location(self) -> DatasetLocation | None:
        """Return the configured AST dataset location, when available.

        Returns:
        -------
        DatasetLocation | None
            AST dataset location when configured.
        """
        return self.profile.resolve_ast_dataset_location()

    def bytecode_dataset_location(self) -> DatasetLocation | None:
        """Return the configured bytecode dataset location, when available.

        Returns:
        -------
        DatasetLocation | None
            Bytecode dataset location when configured.
        """
        return self.profile.resolve_bytecode_dataset_location()

    def extract_dataset_location(self, name: str) -> DatasetLocation | None:
        """Return a configured extract dataset location for the dataset name.

        Returns:
        -------
        DatasetLocation | None
            Dataset location when configured.
        """
        location = self.profile.resolve_dataset_template(name)
        if location is None:
            from datafusion_engine.session.runtime_dataset_io import (
                extract_output_locations_for_profile,
            )

            location = extract_output_locations_for_profile(self.profile).get(name)
        if location is None:
            location = self.profile.data_sources.extract_output.scip_dataset_locations.get(name)
        if location is None:
            return None
        if location.dataset_spec is None:
            from datafusion_engine.extract.registry import dataset_spec as extract_dataset_spec

            try:
                spec = extract_dataset_spec(name)
            except KeyError:
                spec = None
            if spec is not None:
                location = msgspec.structs.replace(location, dataset_spec=spec)
        return location

    def dataset_location(
        self,
        name: str,
        *,
        dataset_resolver: ManifestDatasetResolver,
    ) -> DatasetLocation | None:
        """Return a configured dataset location for the dataset name.

        Parameters
        ----------
        name
            Dataset name to resolve.
        dataset_resolver
            Pre-resolved manifest resolver.

        Returns:
        -------
        DatasetLocation | None
            Dataset location when configured.
        """
        from datafusion_engine.delta.store_policy import apply_delta_store_policy

        location = self.extract_dataset_location(name)
        if location is not None:
            resolved = apply_delta_store_policy(
                location,
                policy=self.profile.policies.delta_store_policy,
            )
            if self.profile.policies.scan_policy is None:
                return resolved
            from datafusion_engine.dataset.policies import apply_scan_policy_defaults
            from datafusion_engine.dataset.registry import (
                DatasetLocationOverrides,
                resolve_dataset_policies,
            )

            policies = resolve_dataset_policies(resolved, overrides=resolved.overrides)
            datafusion_scan, delta_scan = apply_scan_policy_defaults(
                dataset_format=resolved.format or "delta",
                datafusion_scan=policies.datafusion_scan,
                delta_scan=policies.delta_scan,
                policy=self.profile.policies.scan_policy,
            )
            if datafusion_scan is None and delta_scan is None:
                return resolved
            overrides = resolved.overrides or DatasetLocationOverrides()
            if datafusion_scan is not None:
                overrides = msgspec.structs.replace(overrides, datafusion_scan=datafusion_scan)
            if delta_scan is not None:
                delta_bundle = policies.delta_bundle
                if delta_bundle is None:
                    from schema_spec.dataset_spec import DeltaPolicyBundle as _DeltaPolicyBundle

                    delta_bundle = _DeltaPolicyBundle(scan=delta_scan)
                else:
                    delta_bundle = msgspec.structs.replace(delta_bundle, scan=delta_scan)
                overrides = msgspec.structs.replace(overrides, delta=delta_bundle)
            return msgspec.structs.replace(resolved, overrides=overrides)
        return dataset_resolver.location(name)

    def dataset_location_or_raise(
        self,
        name: str,
        *,
        dataset_resolver: ManifestDatasetResolver,
    ) -> DatasetLocation:
        """Return a configured dataset location for the dataset name.

        Parameters
        ----------
        name
            Dataset name to resolve.
        dataset_resolver
            Pre-resolved manifest resolver.

        Returns:
        -------
        DatasetLocation
            The resolved dataset location.

        Raises:
            KeyError: When no location is configured for the dataset name.
        """
        location = self.dataset_location(name, dataset_resolver=dataset_resolver)
        if location is None:
            msg = f"No dataset location configured for {name!r}."
            raise KeyError(msg)
        return location


class _RuntimeProfileIOFacadeMixin:
    """Facade methods for runtime-profile I/O operations."""

    def cache_root(self) -> str:
        """Return the configured cache root directory.

        Returns:
        -------
        str
            Cache root directory.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        return profile.io_ops.cache_root()


class _RuntimeProfileCatalogFacadeMixin:
    """Facade methods for runtime-profile catalog operations."""

    @staticmethod
    def dataset_location(
        name: str,
        *,
        dataset_resolver: ManifestDatasetResolver,
    ) -> DatasetLocation | None:
        """Return a configured dataset location for the dataset name.

        Parameters
        ----------
        name
            Dataset name to resolve.
        dataset_resolver
            Pre-resolved manifest resolver.

        Returns:
        -------
        DatasetLocation | None
            Dataset location when configured.
        """
        return dataset_resolver.location(name)


class _RuntimeProfileDeltaFacadeMixin:
    """Facade methods for runtime-profile Delta operations."""

    def delta_service(self) -> DeltaServicePort:
        """Return a DeltaService bound to this runtime profile.

        Returns:
        -------
        DeltaServicePort
            DeltaService instance bound to this profile.
        """
        profile = cast("DataFusionRuntimeProfile", self)
        return profile.delta_ops.delta_service()


def _delta_commit_spec() -> ArtifactSpec:
    """Return the delta commit artifact spec (deferred import)."""
    from serde_artifact_specs import DATAFUSION_DELTA_COMMIT_SPEC

    return DATAFUSION_DELTA_COMMIT_SPEC


# Avoid circular import: DataFusionRuntimeProfile is used by type annotations only.
if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
