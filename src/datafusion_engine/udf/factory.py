"""FunctionFactory helpers for optional DataFusion extensions."""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.arrow.field_builders import (
    bool_field,
    int32_field,
    list_field,
    string_field,
)
from datafusion_engine.extensions.context_adaptation import (
    ExtensionEntrypointInvocation,
    invoke_entrypoint_with_adapted_context,
)
from storage.ipc_utils import payload_ipc_bytes

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.udf.catalog import DataFusionUdfSpec

UdfVolatility = Literal["immutable", "stable", "volatile"]

POLICY_PAYLOAD_VERSION: int = 1

_PARAMETER_SCHEMA = pa.struct(
    [
        string_field("name"),
        string_field("dtype"),
    ]
)
_PRIMITIVE_SCHEMA = pa.struct(
    [
        string_field("name"),
        list_field("params", _PARAMETER_SCHEMA),
        string_field("return_type"),
        string_field("volatility"),
        string_field("description"),
        bool_field("supports_named_args"),
    ]
)
_POLICY_SCHEMA = pa.schema(
    [
        int32_field("version"),
        bool_field("prefer_named_arguments"),
        bool_field("allow_async"),
        list_field("domain_operator_hooks", pa.string()),
        list_field("primitives", _PRIMITIVE_SCHEMA),
    ]
)


@dataclass(frozen=True)
class FunctionParameter:
    """Signature for a scalar function parameter."""

    name: str
    dtype: str

    def to_payload(self) -> dict[str, str]:
        """Return a payload representation.

        Returns:
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

        Returns:
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
class FunctionArgSpec:
    """Argument definition for CREATE FUNCTION statements."""

    name: str
    dtype: str


@dataclass(frozen=True)
class CreateFunctionConfig:
    """Configuration for CREATE FUNCTION statements."""

    name: str
    args: tuple[FunctionArgSpec, ...] = ()
    return_type: str | None = None
    returns_table: bool = False
    body_sql: str | None = None
    language: str | None = None
    volatility: str | None = None
    replace: bool = False


@dataclass(frozen=True)
class UdafSpecInput:
    """Inputs needed to build a UDAF rule primitive."""

    name: str
    accumulator_class: type
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    state_type: pa.DataType | None = None
    volatility: UdfVolatility = "stable"
    description: str | None = None


@dataclass(frozen=True)
class UdwfSpecInput:
    """Inputs needed to build a UDWF rule primitive."""

    name: str
    evaluator_class: type
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    volatility: UdfVolatility = "stable"
    description: str | None = None


@dataclass(frozen=True)
class FunctionFactoryPolicy(FingerprintableConfig):
    """Policy options for FunctionFactory registration."""

    primitives: tuple[RulePrimitive, ...] = ()
    prefer_named_arguments: bool = True
    allow_async: bool = False
    domain_operator_hooks: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        """Return a payload mapping for the policy.

        Returns:
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

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the FunctionFactory policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing FunctionFactory policy settings.
        """
        return self.to_payload()

    def fingerprint(self) -> str:
        """Return fingerprint for the FunctionFactory policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


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
        if hasattr(module, "install_function_factory"):
            return module
    msg = "A DataFusion extension module with FunctionFactory hooks is required."
    raise ImportError(msg)


def _install_native_function_factory(ctx: SessionContext, *, payload: bytes) -> None:
    """Install the native FunctionFactory into the session.

    Args:
        ctx: Description.
        payload: Description.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    module = _load_extension()
    install = getattr(module, "install_function_factory", None)
    if not callable(install):
        msg = "DataFusion extension entrypoint install_function_factory is unavailable."
        raise TypeError(msg)
    invoke_entrypoint_with_adapted_context(
        getattr(module, "__name__", "unknown"),
        module,
        "install_function_factory",
        ExtensionEntrypointInvocation(
            ctx=ctx,
            internal_ctx=getattr(ctx, "ctx", None),
            args=(payload,),
            allow_fallback=False,
        ),
    )


def install_function_factory(
    ctx: SessionContext,
    *,
    policy: FunctionFactoryPolicy | None = None,
) -> None:
    """Install FunctionFactory hooks for global rule primitives.

    Args:
        ctx: Description.
        policy: Description.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    payload = _policy_payload(policy or FunctionFactoryPolicy())
    try:
        _install_native_function_factory(ctx, payload=payload)
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = (
            "FunctionFactory install failed due to SessionContext type mismatch. "
            "Rebuild and install matching datafusion/datafusion_ext wheels "
            "(scripts/build_datafusion_wheels.sh + uv sync)."
        )
        raise TypeError(msg) from exc


def function_factory_payloads(
    policy: FunctionFactoryPolicy | None = None,
) -> Mapping[str, object]:
    """Return a structured payload for observability and tests.

    Returns:
    -------
    Mapping[str, object]
        Structured payload of function factory policy settings.
    """
    resolved = policy or FunctionFactoryPolicy()
    return resolved.to_payload()


def function_factory_policy_hash(
    snapshot: Mapping[str, object],
    *,
    allow_async: bool = False,
) -> str:
    """Return the fingerprint hash for a snapshot-derived policy.

    Returns:
    -------
    str
        Fingerprint hash for the derived policy.
    """
    policy = function_factory_policy_from_snapshot(snapshot, allow_async=allow_async)
    return policy.fingerprint()


def function_factory_policy_from_snapshot(
    snapshot: Mapping[str, object],
    *,
    allow_async: bool = False,
) -> FunctionFactoryPolicy:
    """Return a FunctionFactory policy derived from a Rust UDF snapshot.

    Parameters
    ----------
    snapshot:
        Rust UDF registry snapshot payload.
    allow_async:
        Whether async UDF execution should be enabled.

    Returns:
    -------
    FunctionFactoryPolicy
        Policy derived from the snapshot metadata.

    Raises:
        RuntimeError: If Rust policy derivation entrypoint is unavailable.
    """
    extension_payload = _derive_policy_payload_from_snapshot(snapshot, allow_async=allow_async)
    if extension_payload is None:
        msg = (
            "FunctionFactory policy derivation requires Rust extension entrypoint "
            "derive_function_factory_policy."
        )
        raise RuntimeError(msg)
    return _policy_from_payload(extension_payload)


def _derive_policy_payload_from_snapshot(
    snapshot: Mapping[str, object],
    *,
    allow_async: bool,
) -> Mapping[str, object] | None:
    module = _load_extension()
    derive = getattr(module, "derive_function_factory_policy", None)
    if not callable(derive):
        return None
    payload = derive(snapshot, allow_async)
    if isinstance(payload, Mapping):
        return dict(payload)
    return None


def _payload_sequence(payload: object) -> tuple[object, ...]:
    if isinstance(payload, Mapping):
        payload = payload.values()
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return tuple(payload)
    return ()


def _params_from_payload(params_payload: object) -> tuple[FunctionParameter, ...]:
    params: list[FunctionParameter] = []
    for param_item in _payload_sequence(params_payload):
        if not isinstance(param_item, Mapping):
            continue
        param_name = param_item.get("name")
        dtype = param_item.get("dtype")
        if isinstance(param_name, str) and isinstance(dtype, str):
            params.append(FunctionParameter(name=param_name, dtype=dtype))
    return tuple(params)


def _primitive_from_payload(item: Mapping[str, object]) -> RulePrimitive | None:
    name = item.get("name")
    return_type = item.get("return_type")
    if not isinstance(name, str) or not isinstance(return_type, str):
        return None
    params = _params_from_payload(item.get("params"))
    volatility_value = item.get("volatility")
    description_value = item.get("description")
    return RulePrimitive(
        name=name,
        params=params,
        return_type=return_type,
        volatility=_normalize_udf_volatility(
            volatility_value if isinstance(volatility_value, str) else None
        ),
        description=description_value if isinstance(description_value, str) else None,
        supports_named_args=bool(item.get("supports_named_args", bool(params))),
    )


def _policy_from_payload(payload: Mapping[str, object]) -> FunctionFactoryPolicy:
    primitives: list[RulePrimitive] = []
    for item in _payload_sequence(payload.get("primitives")):
        if not isinstance(item, Mapping):
            continue
        primitive = _primitive_from_payload(item)
        if primitive is not None:
            primitives.append(primitive)
    prefer_named_arguments = bool(payload.get("prefer_named_arguments", False))
    allow_async = bool(payload.get("allow_async", False))
    hooks = tuple(
        str(item)
        for item in _payload_sequence(payload.get("domain_operator_hooks"))
        if isinstance(item, str)
    )
    return FunctionFactoryPolicy(
        primitives=tuple(primitives),
        prefer_named_arguments=prefer_named_arguments,
        allow_async=allow_async,
        domain_operator_hooks=hooks,
    )


def _rule_primitive_from_spec(spec: DataFusionUdfSpec) -> RulePrimitive:
    arg_names = spec.arg_names
    names: tuple[str, ...] | None = None
    if arg_names and len(arg_names) == len(spec.input_types):
        names = arg_names
    params = tuple(
        FunctionParameter(
            name=names[idx] if names is not None else f"arg{idx}",
            dtype=str(dtype),
        )
        for idx, dtype in enumerate(spec.input_types)
    )
    return RulePrimitive(
        name=spec.engine_name,
        params=params,
        return_type=str(spec.return_type),
        volatility=_normalize_udf_volatility(spec.volatility),
        description=spec.description,
        supports_named_args=names is not None,
    )


def _normalize_udf_volatility(value: str | None) -> UdfVolatility:
    """Normalize UDF volatility values for rule primitives.

    Parameters
    ----------
    value:
        Volatility value from the UDF specification.

    Returns:
    -------
    UdfVolatility
        Normalized volatility literal.
    """
    if value in {"immutable", "stable", "volatile"}:
        return cast("UdfVolatility", value)
    return "stable"


def create_udaf_spec(spec: UdafSpecInput) -> RulePrimitive:
    """Create a rule primitive specification for a UDAF (User-Defined Aggregate Function).

    This helper function creates a standardized RulePrimitive for aggregate functions
    that can be registered in the FunctionFactory policy.

    Parameters
    ----------
    spec:
        UDAF specification input describing the aggregate function.

    Returns:
    -------
    RulePrimitive
        Rule primitive specification for the UDAF.
    """
    _ = spec.accumulator_class, spec.state_type  # Reserved for future metadata extraction
    params = tuple(
        FunctionParameter(name=f"arg{i}", dtype=str(dtype))
        for i, dtype in enumerate(spec.input_types)
    )
    return RulePrimitive(
        name=spec.name,
        params=params,
        return_type=str(spec.return_type),
        volatility=spec.volatility,
        description=spec.description or f"User-defined aggregate function: {spec.name}",
        supports_named_args=True,
    )


def create_udwf_spec(spec: UdwfSpecInput) -> RulePrimitive:
    """Create a rule primitive specification for a UDWF (User-Defined Window Function).

    This helper function creates a standardized RulePrimitive for window functions
    that can be registered in the FunctionFactory policy.

    Parameters
    ----------
    spec:
        UDWF specification input describing the window function.

    Returns:
    -------
    RulePrimitive
        Rule primitive specification for the UDWF.
    """
    _ = spec.evaluator_class  # Reserved for future metadata extraction
    params = tuple(
        FunctionParameter(name=f"arg{i}", dtype=str(dtype))
        for i, dtype in enumerate(spec.input_types)
    )
    return RulePrimitive(
        name=spec.name,
        params=params,
        return_type=str(spec.return_type),
        volatility=spec.volatility,
        description=spec.description or f"User-defined window function: {spec.name}",
        supports_named_args=True,
    )


def register_function(
    ctx: SessionContext,
    *,
    config: CreateFunctionConfig,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> None:
    """Register a SQL macro function using FunctionFactory support.

    Args:
        ctx: DataFusion session context to register the function in.
        config: Function configuration payload.
        runtime_profile: Optional runtime profile for policy enforcement.

    Raises:
        TypeError: If ``sql_with_options`` fails for reasons unrelated to
            SQLOptions ABI compatibility.
    """
    from datafusion_engine.catalog.introspection import invalidate_introspection_cache

    sql = build_create_function_sql(config=config)
    allow_ddl = True
    allow_statements = True
    options = (
        (runtime_profile.sql_options() if runtime_profile else SQLOptions())
        .with_allow_ddl(allow_ddl)
        .with_allow_statements(allow_statements)
    )
    try:
        ctx.sql_with_options(sql, options).collect()
    except TypeError as exc:
        message = str(exc)
        if "cannot be converted to 'SQLOptions'" not in message:
            raise
        ctx.sql(sql).collect()
    invalidate_introspection_cache(ctx)


def build_create_function_sql(*, config: CreateFunctionConfig) -> str:
    """Build CREATE FUNCTION statements as SQL text.

    Args:
        config: Description.

    Returns:
        str: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if not config.body_sql:
        msg = "CREATE FUNCTION requires a function body."
        raise ValueError(msg)
    if config.return_type is None and not config.returns_table:
        msg = "CREATE FUNCTION requires a return type or returns_table=True."
        raise ValueError(msg)
    args_sql = ", ".join(f"{arg.name} {arg.dtype}" for arg in config.args)
    replace_sql = " OR REPLACE" if config.replace else ""
    if config.returns_table:
        returns_sql = "RETURNS TABLE"
    else:
        if config.return_type is None:
            msg = "CREATE FUNCTION requires a return type when returns_table is False."
            raise ValueError(msg)
        returns_sql = f"RETURNS {config.return_type}"
    if config.language:
        language_sql = f" LANGUAGE {config.language}"
        body_keyword = "AS"
    else:
        language_sql = ""
        body_keyword = "RETURN"
    volatility_sql = f" {config.volatility.upper()}" if config.volatility else ""
    return (
        f"CREATE{replace_sql} FUNCTION {config.name}({args_sql}) "
        f"{returns_sql}{language_sql}{volatility_sql} {body_keyword} {config.body_sql}"
    )


__all__ = [
    "CreateFunctionConfig",
    "FunctionArgSpec",
    "FunctionFactoryPolicy",
    "FunctionParameter",
    "RulePrimitive",
    "UdafSpecInput",
    "UdfVolatility",
    "UdwfSpecInput",
    "build_create_function_sql",
    "create_udaf_spec",
    "create_udwf_spec",
    "function_factory_payloads",
    "function_factory_policy_from_snapshot",
    "function_factory_policy_hash",
    "install_function_factory",
    "register_function",
]
