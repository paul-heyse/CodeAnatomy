"""ExprPlanner installation helpers for DataFusion extensions."""

from __future__ import annotations

import importlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from datafusion import SessionContext


@dataclass(frozen=True)
class ExprPlannerPolicy:
    """Policy options for ExprPlanner registration."""

    planner_names: tuple[str, ...]

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-serializable policy payload.

        Returns
        -------
        dict[str, object]
            JSON-serializable policy payload.
        """
        return {"planner_names": list(self.planner_names)}


def default_expr_planner_policy(
    planner_names: Sequence[str],
) -> ExprPlannerPolicy:
    """Return the default ExprPlanner policy.

    Returns
    -------
    ExprPlannerPolicy
        Policy describing ExprPlanner names to install.
    """
    return ExprPlannerPolicy(planner_names=tuple(planner_names))


def _policy_payload(policy: ExprPlannerPolicy) -> str:
    payload = policy.to_payload()
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _load_extension() -> object:
    """Import the native DataFusion extension module.

    Returns
    -------
    object
        Imported datafusion_ext module.

    Raises
    ------
    ImportError
        Raised when the extension module is unavailable.
    ModuleNotFoundError
        Raised when a nested dependency import fails.
    """
    try:
        return importlib.import_module("datafusion_ext")
    except ModuleNotFoundError as exc:
        if exc.name != "datafusion_ext":
            raise
        msg = "datafusion_ext is required for ExprPlanner installation."
        raise ImportError(msg) from exc


def _install_native_expr_planners(
    ctx: SessionContext,
    *,
    planner_names: Sequence[str],
    payload: str,
) -> None:
    module = _load_extension()
    install = getattr(module, "install_expr_planners", None)
    if not callable(install):
        msg = "datafusion_ext.install_expr_planners is unavailable."
        raise TypeError(msg)
    try:
        install(ctx, list(planner_names))
    except TypeError:
        install(ctx, payload)


def install_expr_planners(
    ctx: SessionContext,
    *,
    planner_names: Sequence[str],
) -> None:
    """Install ExprPlanner hooks for named-argument support.

    Parameters
    ----------
    ctx:
        DataFusion SessionContext for extension installation.
    planner_names:
        ExprPlanner identifiers to register.

    Raises
    ------
    ValueError
        Raised when no planner names are provided.
    """
    if not planner_names:
        msg = "ExprPlanner installation requires at least one planner name."
        raise ValueError(msg)
    policy = default_expr_planner_policy(planner_names)
    payload = _policy_payload(policy)
    _install_native_expr_planners(ctx, planner_names=planner_names, payload=payload)


def expr_planner_payloads(planner_names: Sequence[str]) -> Mapping[str, object]:
    """Return a structured payload for ExprPlanner settings.

    Returns
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
