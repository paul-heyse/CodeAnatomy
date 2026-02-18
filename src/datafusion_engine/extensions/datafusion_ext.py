"""Canonical DataFusion extension module wrapper.

This wrapper keeps ``src/datafusion_engine`` on a single extension surface while
normalizing SessionContext wrappers to the native runtime object expected by
extension entrypoints.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from functools import cache
from types import ModuleType
from typing import Any

from datafusion_engine.extensions.required_entrypoints import REQUIRED_RUNTIME_ENTRYPOINTS


def _missing_required_entrypoints(module: ModuleType) -> list[str]:
    return [
        name for name in REQUIRED_RUNTIME_ENTRYPOINTS if not callable(getattr(module, name, None))
    ]


def _load_internal_module() -> tuple[ModuleType, ModuleType | None, bool]:
    try:
        module = importlib.import_module("datafusion._internal")
    except ImportError:
        try:
            stub = importlib.import_module("test_support.datafusion_ext_stub")
        except ImportError:
            msg = "Neither datafusion._internal nor test_support.datafusion_ext_stub is available."
            raise AttributeError(msg) from None
        missing_stub = _missing_required_entrypoints(stub)
        if missing_stub:
            missing_csv = ", ".join(sorted(missing_stub))
            msg = (
                "test_support.datafusion_ext_stub is missing required runtime "
                f"entrypoints: {missing_csv}"
            )
            raise AttributeError(msg) from None
        return stub, None, True

    fallback: ModuleType | None = None
    missing = _missing_required_entrypoints(module)
    if missing:
        try:
            fallback = importlib.import_module("test_support.datafusion_ext_stub")
        except ImportError:
            missing_csv = ", ".join(sorted(missing))
            msg = f"datafusion._internal is missing required runtime entrypoints: {missing_csv}"
            raise AttributeError(msg) from None
        missing_stub = [name for name in missing if not callable(getattr(fallback, name, None))]
        if missing_stub:
            missing_csv = ", ".join(sorted(missing_stub))
            msg = (
                "test_support.datafusion_ext_stub is missing required runtime "
                f"entrypoints: {missing_csv}"
            )
            raise AttributeError(msg)
    return module, fallback, False


_INTERNAL, _FALLBACK_INTERNAL, IS_STUB = _load_internal_module()


def _resolve_attr(name: str) -> Any:
    if hasattr(_INTERNAL, name):
        return getattr(_INTERNAL, name)
    if _FALLBACK_INTERNAL is not None and hasattr(_FALLBACK_INTERNAL, name):
        return getattr(_FALLBACK_INTERNAL, name)
    msg = f"datafusion extension entrypoint is unavailable: {name}"
    raise AttributeError(msg)


def _normalize_ctx(value: Any) -> Any:
    inner = getattr(value, "ctx", None)
    return inner if inner is not None else value


def _normalize_args(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    if not args and "ctx" not in kwargs:
        return args, kwargs
    normalized_args = list(args)
    if normalized_args:
        normalized_args[0] = _normalize_ctx(normalized_args[0])
    normalized_kwargs = dict(kwargs)
    if "ctx" in normalized_kwargs:
        normalized_kwargs["ctx"] = _normalize_ctx(normalized_kwargs["ctx"])
    return tuple(normalized_args), normalized_kwargs


@cache
def _wrapped_attr(name: str) -> Any:
    attr = _resolve_attr(name)
    if not callable(attr):
        return attr

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        call_args, call_kwargs = _normalize_args(args, kwargs)
        return attr(*call_args, **call_kwargs)

    _wrapped.__name__ = getattr(attr, "__name__", name)
    _wrapped.__qualname__ = getattr(attr, "__qualname__", name)
    _wrapped.__doc__ = getattr(attr, "__doc__", None)
    return _wrapped


def _require_internal_callable(name: str) -> Any:
    try:
        attr = _resolve_attr(name)
    except AttributeError as exc:
        msg = f"datafusion extension is missing required entrypoint: {name}"
        raise AttributeError(msg) from exc
    if callable(attr):
        return attr
    msg = f"datafusion extension entrypoint is not callable: {name}"
    raise AttributeError(msg)


def ensure_required_runtime_entrypoints() -> None:
    """Validate that all required runtime entrypoints are available.

    Raises:
        AttributeError: If one or more required runtime entrypoints are missing.
    """
    missing = [
        name
        for name in REQUIRED_RUNTIME_ENTRYPOINTS
        if not callable(getattr(_INTERNAL, name, None))
        and not (
            _FALLBACK_INTERNAL is not None and callable(getattr(_FALLBACK_INTERNAL, name, None))
        )
    ]
    if missing:
        missing_csv = ", ".join(sorted(missing))
        msg = f"datafusion extension is missing required runtime entrypoints: {missing_csv}"
        raise AttributeError(msg)


def _call_required(name: str, *args: Any, **kwargs: Any) -> Any:
    fn = _require_internal_callable(name)
    call_args, call_kwargs = _normalize_args(args, kwargs)
    return fn(*call_args, **call_kwargs)


def install_planner_rules(ctx: Any) -> None:
    """Install planner rules via required extension entrypoint."""
    _call_required("install_planner_rules", ctx)


def install_physical_rules(ctx: Any) -> None:
    """Install physical rules via required extension entrypoint."""
    _call_required("install_physical_rules", ctx)


def install_schema_evolution_adapter_factory(ctx: Any) -> None:
    """Install schema evolution adapter factory via required entrypoint."""
    _call_required("install_schema_evolution_adapter_factory", ctx)


def install_codeanatomy_runtime(
    ctx: Any,
    *,
    enable_async_udfs: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
) -> Mapping[str, object]:
    """Install canonical runtime surfaces and return install snapshot payload.

    Returns:
        Mapping[str, object]: Normalized runtime-install payload.

    Raises:
        TypeError: If the extension response payload is not a mapping.
    """
    payload = _call_required(
        "install_codeanatomy_runtime",
        ctx,
        enable_async_udfs=enable_async_udfs,
        async_udf_timeout_ms=async_udf_timeout_ms,
        async_udf_batch_size=async_udf_batch_size,
    )
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    msg = "install_codeanatomy_runtime returned a non-mapping payload."
    raise TypeError(msg)


def install_tracing(ctx: Any) -> None:
    """Install tracing via required extension entrypoint."""
    _call_required("install_tracing", ctx)


def register_cache_tables(ctx: Any, payload: Any) -> None:
    """Register cache tables via required extension entrypoint."""
    _call_required("register_cache_tables", ctx, payload)


def build_extraction_session(config_payload: Mapping[str, object]) -> Any:
    """Build extraction SessionContext through required extension entrypoint.

    Returns:
        Any: SessionContext-compatible object returned by the extension.
    """
    return _call_required("build_extraction_session", config_payload)


def register_dataset_provider(
    ctx: Any, request_payload: Mapping[str, object]
) -> Mapping[str, object]:
    """Register dataset provider through required extension entrypoint.

    Returns:
        Mapping[str, object]: Normalized provider-registration payload.

    Raises:
        TypeError: If the extension response payload is not a mapping.
    """
    payload = _call_required("register_dataset_provider", ctx, request_payload)
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    msg = "register_dataset_provider returned a non-mapping payload."
    raise TypeError(msg)


def udf_docs_snapshot(ctx: Any) -> dict[str, object]:
    """Return UDF documentation snapshot from required extension entrypoint."""
    payload = _call_required("udf_docs_snapshot", ctx)
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    return {}


def replay_substrait_plan(ctx: Any, payload_bytes: bytes) -> Any:
    """Replay Substrait payloads via required runtime entrypoint.

    Returns:
    -------
    Any
        DataFrame-like result from the runtime extension.
    """
    return _call_required("replay_substrait_plan", ctx, payload_bytes)


def capture_plan_bundle_runtime(
    ctx: Any,
    payload: Mapping[str, object],
    *,
    df: Any,
) -> Mapping[str, object]:
    """Capture runtime plan-bundle payload via required extension entrypoint.

    Returns:
        Mapping[str, object]: Normalized mapping response from extension entrypoint.

    Raises:
        TypeError: If the extension response is not a mapping payload.
    """
    response = _call_required("capture_plan_bundle_runtime", ctx, payload, df)
    if isinstance(response, Mapping):
        return {str(key): value for key, value in response.items()}
    msg = "capture_plan_bundle_runtime returned a non-mapping payload."
    raise TypeError(msg)


def build_plan_bundle_artifact_with_warnings(
    ctx: Any,
    payload: Mapping[str, object],
    *,
    df: Any,
) -> Mapping[str, object]:
    """Build plan-bundle artifact payload via required extension entrypoint.

    Returns:
        Mapping[str, object]: Normalized mapping response from extension entrypoint.

    Raises:
        TypeError: If the extension response is not a mapping payload.
    """
    response = _call_required("build_plan_bundle_artifact_with_warnings", ctx, payload, df)
    if isinstance(response, Mapping):
        return {str(key): value for key, value in response.items()}
    msg = "build_plan_bundle_artifact_with_warnings returned a non-mapping payload."
    raise TypeError(msg)


def __getattr__(name: str) -> Any:
    return _wrapped_attr(name)


ensure_required_runtime_entrypoints()


__all__ = [
    "IS_STUB",
    "build_extraction_session",
    "build_plan_bundle_artifact_with_warnings",
    "capture_plan_bundle_runtime",
    "ensure_required_runtime_entrypoints",
    "install_codeanatomy_runtime",
    "install_physical_rules",
    "install_planner_rules",
    "install_schema_evolution_adapter_factory",
    "install_tracing",
    "register_cache_tables",
    "register_dataset_provider",
    "replay_substrait_plan",
    "udf_docs_snapshot",
]
