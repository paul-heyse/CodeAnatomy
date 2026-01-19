"""FunctionFactory helpers for optional DataFusion extensions."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa
from datafusion import SessionContext

from registry_common.arrow_payloads import payload_ipc_bytes

UdfVolatility = Literal["immutable", "stable", "volatile"]

POLICY_PAYLOAD_VERSION: int = 1

_PARAMETER_SCHEMA = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("dtype", pa.string()),
    ]
)
_PRIMITIVE_SCHEMA = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("params", pa.list_(_PARAMETER_SCHEMA)),
        pa.field("return_type", pa.string()),
        pa.field("volatility", pa.string()),
        pa.field("description", pa.string()),
        pa.field("supports_named_args", pa.bool_()),
    ]
)
_POLICY_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("prefer_named_arguments", pa.bool_()),
        pa.field("allow_async", pa.bool_()),
        pa.field("domain_operator_hooks", pa.list_(pa.string())),
        pa.field("primitives", pa.list_(_PRIMITIVE_SCHEMA)),
    ]
)


@dataclass(frozen=True)
class FunctionParameter:
    """Signature for a scalar function parameter."""

    name: str
    dtype: str

    def to_payload(self) -> dict[str, str]:
        """Return a payload representation.

        Returns
        -------
        dict[str, str]
            Function parameter payload.
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
        """Return a payload representation.

        Returns
        -------
        dict[str, object]
            Rule primitive payload.
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
        """Return a payload mapping for the policy.

        Returns
        -------
        dict[str, object]
            Policy payload mapping.
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


def _policy_payload(policy: FunctionFactoryPolicy) -> bytes:
    payload = policy.to_payload()
    row = {
        "version": POLICY_PAYLOAD_VERSION,
        "prefer_named_arguments": payload["prefer_named_arguments"],
        "allow_async": payload["allow_async"],
        "domain_operator_hooks": payload["domain_operator_hooks"],
        "primitives": payload["primitives"],
    }
    return payload_ipc_bytes(row, _POLICY_SCHEMA)


def _load_extension() -> object:
    """Import the native DataFusion extension module.

    Returns
    -------
    object
        Imported datafusion_ext module.

    Raises
    ------
    ImportError
        Raised when the extension module cannot be imported.
    ModuleNotFoundError
        Raised when a nested dependency import fails.
    """
    try:
        return importlib.import_module("datafusion_ext")
    except ModuleNotFoundError as exc:
        if exc.name != "datafusion_ext":
            raise
        msg = "datafusion_ext is required for native FunctionFactory installation."
        raise ImportError(msg) from exc


def _install_native_function_factory(ctx: SessionContext, *, payload: bytes) -> None:
    """Install the native FunctionFactory into the session.

    Raises
    ------
    TypeError
        Raised when the extension does not expose an install hook.
    """
    module = _load_extension()
    install = getattr(module, "install_function_factory", None)
    if not callable(install):
        msg = "datafusion_ext.install_function_factory is unavailable."
        raise TypeError(msg)
    install(ctx, payload)


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

    """
    payload = _policy_payload(policy or default_function_factory_policy())
    _install_native_function_factory(ctx, payload=payload)


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
