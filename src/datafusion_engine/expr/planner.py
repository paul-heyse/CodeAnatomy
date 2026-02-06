"""ExprPlanner installation helpers for DataFusion extensions."""

from __future__ import annotations

import base64
import importlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext

from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.arrow.schema import version_field
from storage.ipc_utils import payload_ipc_bytes

EXPR_PLANNER_PAYLOAD_VERSION = 1
_EXPR_PLANNER_PAYLOAD_SCHEMA = pa.schema(
    [
        version_field(),
        pa.field("planner_names", pa.list_(pa.string()), nullable=False),
    ]
)


@dataclass(frozen=True)
class ExprPlannerPolicy(FingerprintableConfig):
    """Policy options for ExprPlanner registration."""

    planner_names: tuple[str, ...]

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the ExprPlanner policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing ExprPlanner policy settings.
        """
        return {
            "planner_names": tuple(self.planner_names),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the ExprPlanner policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())

    def to_payload(self) -> dict[str, object]:
        """Return a policy payload for IPC serialization.

        Returns:
        -------
        dict[str, object]
            Policy payload for IPC serialization.
        """
        return {"planner_names": list(self.planner_names)}


def default_expr_planner_policy(
    planner_names: Sequence[str],
) -> ExprPlannerPolicy:
    """Return the default ExprPlanner policy.

    Returns:
    -------
    ExprPlannerPolicy
        Policy describing ExprPlanner names to install.
    """
    return ExprPlannerPolicy(planner_names=tuple(planner_names))


def _policy_payload(policy: ExprPlannerPolicy) -> str:
    payload = {"version": EXPR_PLANNER_PAYLOAD_VERSION, **policy.to_payload()}
    payload_bytes = payload_ipc_bytes(payload, _EXPR_PLANNER_PAYLOAD_SCHEMA)
    return base64.b64encode(payload_bytes).decode("ascii")


def _load_extension() -> object:
    """Import the native DataFusion extension module.

    Returns:
        object: Result.

    Raises:
        ImportError: If no compatible extension module can be loaded.
        ModuleNotFoundError: If a nested import is missing within a candidate module.
    """
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name != module_name:
                raise
            continue
        except ImportError:
            continue
        if hasattr(module, "install_expr_planners"):
            return module
    msg = "A DataFusion extension module with ExprPlanner hooks is required."
    raise ImportError(msg)


def _install_native_expr_planners(
    ctx: SessionContext,
    *,
    planner_names: Sequence[str],
    payload: str,
) -> None:
    module = _load_extension()
    install = getattr(module, "install_expr_planners", None)
    if not callable(install):
        msg = "DataFusion extension entrypoint install_expr_planners is unavailable."
        raise TypeError(msg)
    ctx_arg: object = ctx
    module_name = getattr(module, "__name__", "")
    if module_name in {"datafusion._internal", "datafusion_ext"}:
        internal_ctx = getattr(ctx, "ctx", None)
        if internal_ctx is not None:
            ctx_arg = internal_ctx
    try:
        install(ctx_arg, list(planner_names))
    except TypeError:
        install(ctx_arg, payload)


def _fallback_install_expr_planners(ctx: SessionContext, planner_names: Sequence[str]) -> bool:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:
        return False
    if not getattr(module, "IS_STUB", False):
        return False
    stub_install = getattr(module, "install_expr_planners", None)
    if not callable(stub_install):
        return False
    try:
        stub_install(ctx, list(planner_names))
    except (RuntimeError, TypeError, ValueError):
        return False
    return True


def install_expr_planners(
    ctx: SessionContext,
    *,
    planner_names: Sequence[str],
) -> None:
    """Install ExprPlanner hooks for named-argument support.

    Args:
        ctx: Description.
        planner_names: Description.

    Raises:
        TypeError: If the operation cannot be completed.
        ValueError: If the operation cannot be completed.
    """
    if not planner_names:
        msg = "ExprPlanner installation requires at least one planner name."
        raise ValueError(msg)
    policy = default_expr_planner_policy(planner_names)
    payload = _policy_payload(policy)
    try:
        _install_native_expr_planners(ctx, planner_names=planner_names, payload=payload)
    except TypeError as exc:
        if "cannot be converted" in str(exc) and _fallback_install_expr_planners(
            ctx, planner_names
        ):
            return
        raise


def expr_planner_payloads(planner_names: Sequence[str]) -> Mapping[str, object]:
    """Return a structured payload for ExprPlanner settings.

    Returns:
    -------
    Mapping[str, object]
        Structured payload of expr planner settings.
    """
    return default_expr_planner_policy(planner_names).to_payload()


__all__ = [
    "ExprPlannerPolicy",
    "default_expr_planner_policy",
    "expr_planner_payloads",
    "install_expr_planners",
]
