"""Profile and session-runtime helpers."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_compile import effective_ident_normalization
from datafusion_engine.session.runtime_profile_config import PolicyBundleConfig
from datafusion_engine.session.runtime_session import (
    SessionRuntime,
    session_runtime_for_context,
)


def create_runtime_profile(
    *,
    config_policy_name: str | None = None,
    policies: PolicyBundleConfig | None = None,
) -> DataFusionRuntimeProfile:
    """Construct a runtime profile from canonical policy inputs.

    Returns:
        DataFusionRuntimeProfile: Newly constructed runtime profile.

    Raises:
        ValueError: If both ``config_policy_name`` and explicit ``policies``
            are provided.
    """
    if config_policy_name is not None and policies is not None:
        msg = "create_runtime_profile accepts either config_policy_name or policies, not both."
        raise ValueError(msg)
    resolved_policies = policies
    if resolved_policies is None and config_policy_name is not None:
        resolved_policies = PolicyBundleConfig(config_policy_name=config_policy_name)
    if resolved_policies is None:
        return DataFusionRuntimeProfile()
    return DataFusionRuntimeProfile(policies=resolved_policies)


def runtime_for_profile(
    profile: DataFusionRuntimeProfile,
) -> SessionRuntime:
    """Return session runtime for a profile."""
    return profile.session_runtime()


def runtime_for_context(
    ctx: SessionContext,
    *,
    profile: DataFusionRuntimeProfile,
) -> SessionRuntime:
    """Return session runtime for an explicit context/profile pair.

    Raises:
        RuntimeError: If the runtime cannot be resolved for the provided
            context/profile.
    """
    runtime = session_runtime_for_context(profile, ctx)
    if runtime is None:
        msg = "Failed to build session runtime for provided context/profile."
        raise RuntimeError(msg)
    return runtime


__all__ = [
    "DataFusionRuntimeProfile",
    "SessionRuntime",
    "create_runtime_profile",
    "effective_ident_normalization",
    "runtime_for_context",
    "runtime_for_profile",
]
