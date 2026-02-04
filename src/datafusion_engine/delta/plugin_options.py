"""Helpers for building Delta plugin options from a SessionContext."""

from __future__ import annotations

import base64
import importlib
import json
from collections.abc import Mapping, MutableMapping
from typing import Protocol, cast

from datafusion import SessionContext

from datafusion_engine.errors import DataFusionEngineError, ErrorKind


class _DeltaScanExtension(Protocol):
    def delta_scan_config_from_session(
        self,
        ctx: SessionContext,
        *args: object,
    ) -> Mapping[str, object]:
        """Return Delta scan defaults derived from a SessionContext."""
        ...


def _resolve_delta_extension() -> _DeltaScanExtension:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:
        msg = "Delta plugin options require the datafusion_ext module."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN) from exc
    entrypoint = getattr(module, "delta_scan_config_from_session", None)
    if not callable(entrypoint):
        msg = "Delta scan config entrypoint delta_scan_config_from_session is unavailable."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)
    return cast("_DeltaScanExtension", module)


def _delta_scan_defaults(ctx: SessionContext) -> Mapping[str, object]:
    module = _resolve_delta_extension()
    try:
        payload = module.delta_scan_config_from_session(ctx, None, None, None, None, None)
    except Exception as exc:  # pragma: no cover - error paths depend on extension behavior
        msg = "Failed to derive Delta scan defaults from session."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN) from exc
    if not isinstance(payload, Mapping):
        msg = f"Delta scan defaults payload must be a Mapping, got {type(payload).__name__}."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)
    return payload


def _encode_schema_ipc(scan_config: MutableMapping[str, object]) -> None:
    value = scan_config.get("schema_ipc")
    if value is None:
        return
    if isinstance(value, (bytes, bytearray, memoryview)):
        scan_config["schema_ipc"] = base64.b64encode(bytes(value)).decode("ascii")


def delta_plugin_options_from_session(
    ctx: SessionContext,
    options: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return plugin options with session-derived Delta scan config injected.

    Parameters
    ----------
    ctx
        DataFusion session context that supplies Delta scan defaults.
    options
        Optional plugin options to merge with the session defaults.

    Returns
    -------
    dict[str, object]
        Resolved plugin options with validated defaults applied.

    Raises
    ------
    DataFusionEngineError
        If options are invalid or defaults cannot be resolved.
    """
    if options is None:
        resolved: dict[str, object] = {}
    elif isinstance(options, Mapping):
        resolved = dict(options)
    else:
        msg = f"delta plugin options must be a Mapping, got {type(options).__name__}."
        raise DataFusionEngineError(msg, kind=ErrorKind.VALIDATION)

    defaults = dict(_delta_scan_defaults(ctx))
    scan_config = resolved.get("scan_config")
    if scan_config is None:
        merged: dict[str, object] = defaults
    elif isinstance(scan_config, Mapping):
        merged = dict(defaults)
        merged.update(scan_config)
    else:
        msg = f"scan_config must be a Mapping, got {type(scan_config).__name__}."
        raise DataFusionEngineError(msg, kind=ErrorKind.VALIDATION)
    _encode_schema_ipc(merged)
    resolved["scan_config"] = merged
    return resolved


def delta_plugin_options_json(
    ctx: SessionContext,
    options: Mapping[str, object] | None = None,
) -> str:
    """Return JSON-encoded plugin options with session defaults injected.

    Parameters
    ----------
    ctx
        DataFusion session context that supplies Delta scan defaults.
    options
        Optional plugin options to merge with the session defaults.

    Returns
    -------
    str
        JSON-encoded plugin options.
    """
    resolved = delta_plugin_options_from_session(ctx, options)
    return json.dumps(resolved, sort_keys=True)


__all__ = ["delta_plugin_options_from_session", "delta_plugin_options_json"]
