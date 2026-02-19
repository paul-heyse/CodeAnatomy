"""Substrait bridge and validation helpers for plan bundles."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from datafusion_engine.plan.contracts import PlanArtifactPolicyV1

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.session.runtime_session import SessionRuntime


def _required_udfs_from_rust_artifact(artifact: Mapping[str, object]) -> tuple[str, ...] | None:
    raw = artifact.get("required_udfs")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return None
    names = [str(item).strip() for item in raw if str(item).strip()]
    if not names:
        return None
    return tuple(sorted(set(names)))


def substrait_bytes_from_rust_bundle(
    ctx: SessionContext,
    df: DataFrame,
    *,
    session_runtime: SessionRuntime | None,
) -> tuple[bytes, tuple[str, ...] | None]:
    """Build Substrait bytes from the canonical Rust plan-bundle bridge.

    Returns:
        tuple[bytes, tuple[str, ...] | None]: Substrait bytes and required UDF names.

    Raises:
        TypeError: If the Rust bridge payload shape is invalid.
        ValueError: If the Rust bridge payload is missing Substrait bytes.
    """
    from datafusion_engine.plan.rust_bundle_bridge import build_plan_bundle_artifact_with_warnings

    policy = PlanArtifactPolicyV1()
    if policy.cross_process_format != "substrait":
        msg = "cross-process plan artifact policy must use Substrait serialization."
        raise ValueError(msg)
    payload: dict[str, object] = {
        "capture_substrait": True,
        "capture_sql": False,
        "capture_delta_codec": False,
        "deterministic_inputs": True,
        "no_volatile_udfs": True,
        "deterministic_optimizer": True,
    }
    if session_runtime is not None:
        payload["stats_quality"] = "runtime_profile"
    response = build_plan_bundle_artifact_with_warnings(ctx, payload, df=df)
    artifact = response.get("artifact")
    if not isinstance(artifact, Mapping):
        msg = "Rust plan-bundle bridge returned a non-mapping artifact payload."
        raise TypeError(msg)
    raw = artifact.get("substrait_bytes")
    required_udfs = _required_udfs_from_rust_artifact(artifact)
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw), required_udfs
    if isinstance(raw, list):
        try:
            return bytes(int(item) for item in raw), required_udfs
        except (TypeError, ValueError):
            msg = "Rust plan-bundle bridge returned malformed substrait byte values."
            raise ValueError(msg) from None
    msg = "Rust plan-bundle artifact is missing substrait_bytes."
    raise ValueError(msg)


def substrait_validation_payload(
    substrait_bytes: bytes,
    *,
    df: DataFrame,
    ctx: SessionContext,
) -> Mapping[str, object] | None:
    """Validate Substrait bytes and return validation payload when enabled.

    Returns:
        Mapping[str, object] | None: Validation payload when validation is available.

    Raises:
        ValueError: If Substrait replay validation reports a mismatch.
    """
    from datafusion_engine.plan.result_types import validate_substrait_plan

    validation = validate_substrait_plan(substrait_bytes, df=df, ctx=ctx)
    if validation is None:
        return None
    match = validation.get("match")
    if match is False:
        msg = f"Substrait validation failed: {validation}"
        raise ValueError(msg)
    return validation


__all__ = ["substrait_bytes_from_rust_bundle", "substrait_validation_payload"]
