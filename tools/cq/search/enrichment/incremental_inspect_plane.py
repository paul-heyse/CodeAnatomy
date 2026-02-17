"""Incremental inspect enrichment plane."""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from types import ModuleType
from typing import TypeGuard

_MAX_MEMBER_ROWS = 96


def _is_annotation_target(
    obj: object,
) -> TypeGuard[Callable[..., object] | type[object] | ModuleType]:
    return callable(obj) or isinstance(obj, (type, ModuleType))


def _is_callable_target(obj: object) -> TypeGuard[Callable[..., object]]:
    return callable(obj)


def _descriptor_flags(value: object) -> dict[str, bool]:
    return {
        "has_get": callable(getattr(value, "__get__", None)),
        "has_set": callable(getattr(value, "__set__", None)),
        "has_delete": callable(getattr(value, "__delete__", None)),
    }


def _safe_annotations(obj: object) -> dict[str, object]:
    if not _is_annotation_target(obj):
        return {}
    try:
        rows = inspect.get_annotations(obj, eval_str=False)
    except (RuntimeError, TypeError, ValueError, NameError):
        return {}
    if isinstance(rows, dict):
        return {str(key): value for key, value in rows.items() if isinstance(key, str)}
    return {}


def _safe_signature(
    obj: object,
    *,
    follow_wrapped: bool,
) -> tuple[str | None, inspect.Signature | None]:
    if not _is_callable_target(obj):
        return None, None
    try:
        signature = inspect.signature(
            obj,
            follow_wrapped=follow_wrapped,
            eval_str=False,
        )
    except (RuntimeError, TypeError, ValueError):
        return None, None
    return str(signature), signature


def _bind_simulation(signature: inspect.Signature | None) -> dict[str, object]:
    if signature is None:
        return {
            "bind_ok": False,
            "reason": "signature_unavailable",
        }
    args: list[object] = []
    kwargs: dict[str, object] = {}
    sentinel = object()
    for param in signature.parameters.values():
        if (
            param.kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
            and param.default is inspect.Signature.empty
        ):
            args.append(sentinel)
        elif (
            param.kind == inspect.Parameter.KEYWORD_ONLY
            and param.default is inspect.Signature.empty
            and isinstance(param.name, str)
        ):
            kwargs[param.name] = sentinel
    try:
        signature.bind_partial(*args, **kwargs)
    except TypeError as exc:
        return {
            "bind_ok": False,
            "reason": str(exc),
            "required_args_n": len(args),
            "required_kwargs_n": len(kwargs),
        }
    return {
        "bind_ok": True,
        "required_args_n": len(args),
        "required_kwargs_n": len(kwargs),
    }


def _runtime_state_payload(obj: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": type(obj).__name__,
    }
    if inspect.isgenerator(obj):
        payload["generator_state"] = inspect.getgeneratorstate(obj)
        payload["generator_locals"] = sorted(inspect.getgeneratorlocals(obj).keys())
    elif inspect.iscoroutine(obj):
        payload["coroutine_state"] = inspect.getcoroutinestate(obj)
        payload["coroutine_locals"] = sorted(inspect.getcoroutinelocals(obj).keys())
    elif inspect.isasyncgen(obj):
        get_state = getattr(inspect, "getasyncgenstate", None)
        if callable(get_state):
            payload["async_generator_state"] = get_state(obj)
    elif inspect.isframe(obj):
        payload["frame_lineno"] = int(obj.f_lineno)
        payload["frame_name"] = str(obj.f_code.co_name)
    elif inspect.istraceback(obj):
        payload["traceback_last_line"] = int(obj.tb_lineno)
        payload["traceback_last_name"] = str(obj.tb_frame.f_code.co_name)
    return payload


def _member_rows(obj: object) -> list[dict[str, object]]:
    try:
        members = inspect.getmembers_static(obj)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return []
    rows: list[dict[str, object]] = []
    for name, value in members[:_MAX_MEMBER_ROWS]:
        rows.append(
            {
                "name": name,
                "member_type": type(value).__name__,
                "is_routine": bool(inspect.isroutine(value)),
                "is_function": bool(inspect.isfunction(value)),
                "is_method_descriptor": bool(inspect.ismethoddescriptor(value)),
                "is_data_descriptor": bool(inspect.isdatadescriptor(value)),
                "descriptor": _descriptor_flags(value),
            }
        )
    return rows


def inspect_object_inventory(module_name: str, dotted: str) -> dict[str, object]:
    """Inspect static object metadata for a module+dotted path.

    Returns:
        dict[str, object]: Static inspection inventory for the resolved object.
    """
    module = importlib.import_module(module_name)
    obj: object = module
    for part in dotted.split("."):
        obj = inspect.getattr_static(obj, part)

    members = _member_rows(obj)
    sig_wrapped_text, sig_wrapped = _safe_signature(obj, follow_wrapped=True)
    sig_raw_text, _sig_raw = _safe_signature(obj, follow_wrapped=False)
    annotations = _safe_annotations(obj)
    unwrapped: object
    if _is_callable_target(obj):
        unwrapped = inspect.unwrap(obj, stop=lambda current: hasattr(current, "__signature__"))
    else:
        unwrapped = obj

    return {
        "dotted": dotted,
        "module_name": module_name,
        "object_kind": type(obj).__name__,
        "sig_follow_wrapped": sig_wrapped_text,
        "sig_raw": sig_raw_text,
        "unwrapped_qualname": getattr(unwrapped, "__qualname__", ""),
        "annotations": annotations,
        "annotations_keys": sorted(annotations),
        "members_count": len(members),
        "members": members,
        "callsite_bind_check": _bind_simulation(sig_wrapped),
        "runtime": _runtime_state_payload(obj),
    }


def build_inspect_bundle(
    *,
    module_name: str | None,
    dotted_name: str | None,
    runtime_enabled: bool,
) -> dict[str, object]:
    """Build inspect-plane payload with capability-gated runtime probing.

    Returns:
        dict[str, object]: Inspect-plane payload with status and detail data.
    """
    if not runtime_enabled:
        return {
            "status": "skipped",
            "reason": "runtime_disabled",
        }
    if not isinstance(module_name, str) or not module_name:
        return {
            "status": "skipped",
            "reason": "module_unavailable",
        }
    if not isinstance(dotted_name, str) or not dotted_name:
        return {
            "status": "skipped",
            "reason": "dotted_name_unavailable",
        }
    try:
        inventory = inspect_object_inventory(module_name, dotted_name)
    except (
        ModuleNotFoundError,
        AttributeError,
        ImportError,
        RuntimeError,
        ValueError,
        TypeError,
    ) as exc:
        return {
            "status": "degraded",
            "reason": type(exc).__name__,
            "module_name": module_name,
            "dotted_name": dotted_name,
        }
    return {
        "status": "applied",
        "object_inventory": {
            "module_name": inventory.get("module_name"),
            "dotted": inventory.get("dotted"),
            "object_kind": inventory.get("object_kind"),
            "members_count": inventory.get("members_count"),
            "annotations_keys": inventory.get("annotations_keys"),
            "sig_follow_wrapped": inventory.get("sig_follow_wrapped"),
            "sig_raw": inventory.get("sig_raw"),
            "unwrapped_qualname": inventory.get("unwrapped_qualname"),
        },
        "members": inventory.get("members"),
        "callsite_bind_check": inventory.get("callsite_bind_check"),
        "runtime_trace_attribution": inventory.get("runtime"),
    }


__all__ = [
    "build_inspect_bundle",
    "inspect_object_inventory",
]
