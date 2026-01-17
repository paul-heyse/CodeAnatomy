"""Runtime profile presets and helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace

from arrowdsl.core.context import DeterminismTier, RuntimeProfile, runtime_profile_factory


def _cpu_count() -> int:
    count = os.cpu_count()
    return count if count is not None and count > 0 else 1


@dataclass(frozen=True)
class RuntimeProfileSpec:
    """Runtime profile plus compiler/engine preferences."""

    name: str
    runtime: RuntimeProfile
    ibis_fuse_selects: bool

    @property
    def datafusion_settings_hash(self) -> str | None:
        """Return DataFusion settings hash when configured."""
        if self.runtime.datafusion is None:
            return None
        return self.runtime.datafusion.settings_hash()


def _apply_profile_overrides(name: str, runtime: RuntimeProfile) -> RuntimeProfile:
    df_profile = runtime.datafusion
    if df_profile is None:
        return runtime
    if name == "dev_debug":
        runtime = replace(
            runtime,
            cpu_threads=min(_cpu_count(), 4),
            io_threads=min(_cpu_count() * 2, 8),
        )
        df_profile = replace(
            df_profile,
            config_policy_name="dev",
            target_partitions=min(_cpu_count(), 8),
            batch_size=4096,
            capture_explain=True,
            explain_analyze=True,
            explain_analyze_level="dev",
        )
    elif name == "prod_fast":
        df_profile = replace(
            df_profile,
            config_policy_name="prod",
            capture_explain=False,
            explain_analyze=False,
            explain_analyze_level=None,
        )
    elif name == "memory_tight":
        runtime = replace(
            runtime,
            cpu_threads=min(_cpu_count(), 2),
            io_threads=min(_cpu_count(), 4),
        )
        df_profile = replace(
            df_profile,
            config_policy_name="default",
            target_partitions=min(_cpu_count(), 4),
            batch_size=4096,
            capture_explain=False,
            explain_analyze=False,
            explain_analyze_level="summary",
        )
    return runtime.with_datafusion(df_profile)


def resolve_runtime_profile(
    profile: str,
    *,
    determinism: DeterminismTier | None = None,
) -> RuntimeProfileSpec:
    """Return a runtime profile spec for the requested profile name.

    Returns
    -------
    RuntimeProfileSpec
        Resolved runtime profile spec.
    """
    runtime = runtime_profile_factory(profile)
    if determinism is not None:
        runtime = runtime.with_determinism(determinism)
    runtime = _apply_profile_overrides(profile, runtime)
    return RuntimeProfileSpec(
        name=profile,
        runtime=runtime,
        ibis_fuse_selects=runtime.ibis_fuse_selects,
    )


__all__ = ["RuntimeProfileSpec", "resolve_runtime_profile"]
