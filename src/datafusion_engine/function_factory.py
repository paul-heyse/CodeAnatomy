"""FunctionFactory helpers for optional DataFusion extensions."""

from __future__ import annotations

import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from datafusion import SessionContext

UdfVolatility = Literal["immutable", "stable", "volatile"]


@dataclass(frozen=True)
class FunctionParameter:
    """Signature for a scalar function parameter."""

    name: str
    dtype: str

    def to_payload(self) -> dict[str, str]:
        """Return a JSON-serializable representation.

        Returns
        -------
        dict[str, str]
            JSON-serializable function parameter payload.
        """
        return {"name": self.name, "dtype": self.dtype}


@dataclass(frozen=True)
class RulePrimitive:
    """Definition for a global rule primitive."""

    name: str
    params: tuple[FunctionParameter, ...]
    return_type: str
    volatility: UdfVolatility = "stable"
    description: str | None = None
    supports_named_args: bool = True

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-serializable representation.

        Returns
        -------
        dict[str, object]
            JSON-serializable rule primitive payload.
        """
        return {
            "name": self.name,
            "params": [param.to_payload() for param in self.params],
            "return_type": self.return_type,
            "volatility": self.volatility,
            "description": self.description,
            "supports_named_args": self.supports_named_args,
        }


@dataclass(frozen=True)
class FunctionFactoryPolicy:
    """Policy options for FunctionFactory registration."""

    primitives: tuple[RulePrimitive, ...] = ()
    prefer_named_arguments: bool = True
    allow_async: bool = False
    domain_operator_hooks: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-serializable policy payload.

        Returns
        -------
        dict[str, object]
            JSON-serializable policy payload.
        """
        return {
            "primitives": [primitive.to_payload() for primitive in self.primitives],
            "prefer_named_arguments": self.prefer_named_arguments,
            "allow_async": self.allow_async,
            "domain_operator_hooks": list(self.domain_operator_hooks),
        }


DEFAULT_RULE_PRIMITIVES: tuple[RulePrimitive, ...] = (
    RulePrimitive(
        name="cpg_score",
        params=(FunctionParameter(name="value", dtype="float64"),),
        return_type="float64",
        volatility="stable",
        description="Placeholder scoring primitive for relationship planning.",
        supports_named_args=True,
    ),
    RulePrimitive(
        name="stable_hash64",
        params=(FunctionParameter(name="value", dtype="string"),),
        return_type="int64",
        volatility="stable",
        description="Stable 64-bit hash for string inputs.",
        supports_named_args=True,
    ),
    RulePrimitive(
        name="stable_hash128",
        params=(FunctionParameter(name="value", dtype="string"),),
        return_type="string",
        volatility="stable",
        description="Stable 128-bit hash for string inputs.",
        supports_named_args=True,
    ),
    RulePrimitive(
        name="position_encoding_norm",
        params=(FunctionParameter(name="value", dtype="string"),),
        return_type="int32",
        volatility="stable",
        description="Normalize position encoding values to enum integers.",
        supports_named_args=True,
    ),
    RulePrimitive(
        name="col_to_byte",
        params=(
            FunctionParameter(name="line_text", dtype="string"),
            FunctionParameter(name="col", dtype="int64"),
            FunctionParameter(name="col_unit", dtype="string"),
        ),
        return_type="int64",
        volatility="stable",
        description="Convert line/offset pairs into UTF-8 byte offsets.",
        supports_named_args=True,
    ),
)


def default_function_factory_policy() -> FunctionFactoryPolicy:
    """Return the default FunctionFactory policy.

    Returns
    -------
    FunctionFactoryPolicy
        Default policy used for FunctionFactory registration.
    """
    return FunctionFactoryPolicy(
        primitives=DEFAULT_RULE_PRIMITIVES,
        prefer_named_arguments=True,
        allow_async=False,
        domain_operator_hooks=(),
    )


def _policy_payload(policy: FunctionFactoryPolicy) -> str:
    payload = policy.to_payload()
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _load_extension() -> object | None:
    try:
        return importlib.import_module("datafusion_ext")
    except ImportError:
        return None


def install_function_factory(
    ctx: SessionContext,
    *,
    policy: FunctionFactoryPolicy | None = None,
) -> None:
    """Install FunctionFactory hooks for global rule primitives.

    Parameters
    ----------
    ctx:
        DataFusion SessionContext for future registration hooks.
    policy:
        Optional policy for primitive registration.

    Raises
    ------
    RuntimeError
        Raised when the extension is unavailable or refuses registration.
    TypeError
        Raised when the extension installer is missing or not callable.
    """
    _ = ctx
    module = _load_extension()
    if module is None:
        msg = "FunctionFactory extension is unavailable."
        raise RuntimeError(msg)
    installer = getattr(module, "install_function_factory", None)
    if not callable(installer):
        msg = "FunctionFactory installer is unavailable in the extension module."
        raise TypeError(msg)
    installed = installer()
    if not installed:
        msg = "FunctionFactory extension refused to install."
        raise RuntimeError(msg)
    registrar = getattr(module, "register_rule_primitives_json", None)
    if callable(registrar):
        registrar(_policy_payload(policy or default_function_factory_policy()))


def function_factory_payloads(
    policy: FunctionFactoryPolicy | None = None,
) -> Mapping[str, object]:
    """Return a structured payload for observability and tests.

    Returns
    -------
    Mapping[str, object]
        Structured payload of function factory policy settings.
    """
    resolved = policy or default_function_factory_policy()
    return resolved.to_payload()


__all__ = [
    "DEFAULT_RULE_PRIMITIVES",
    "FunctionFactoryPolicy",
    "FunctionParameter",
    "RulePrimitive",
    "UdfVolatility",
    "default_function_factory_policy",
    "function_factory_payloads",
    "install_function_factory",
]
