"""Canonical contract codec and conversion helpers for CQ boundaries."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import cast

import msgspec

from tools.cq.core.contracts_constraints import enforce_mapping_constraints
from tools.cq.core.schema import Artifact, CqResult, Finding, RunMeta, Section
from tools.cq.core.summary_contract import (
    SummaryV1,
    SummaryVariantName,
    resolve_summary_variant_name,
    summary_class_for_variant,
    summary_from_mapping,
)

_RUN_SUMMARY_HINTS = frozenset({"plan_version", "plan"})
_NEIGHBORHOOD_SUMMARY_HINTS = frozenset(
    {
        "target_resolution_kind",
        "top_k",
        "enable_semantic_enrichment",
        "bundle_id",
        "semantic_health",
        "semantic_quiescent",
        "semantic_position_encoding",
        "total_slices",
        "total_diagnostics",
        "total_nodes",
        "total_edges",
    }
)
_CALLS_SUMMARY_HINTS = frozenset(
    {
        "signature",
        "total_sites",
        "files_with_calls",
        "total_py_files",
        "candidate_files",
        "rg_candidates",
        "fallback_count",
        "call_records",
    }
)
_IMPACT_SUMMARY_HINTS = frozenset(
    {
        "parameter",
        "new_signature",
        "taint_sites",
        "max_depth",
        "functions_analyzed",
        "callers_found",
        "would_break",
        "ambiguous",
        "ok",
        "call_sites",
    }
)


def _infer_summary_variant(
    summary_mapping: Mapping[str, object],
    *,
    macro: str | None,
) -> SummaryVariantName:
    if isinstance(macro, str) and macro:
        return resolve_summary_variant_name(
            mode=summary_mapping.get("mode"),
            explicit=summary_mapping.get("summary_variant"),
            macro=macro,
        )

    keys = {key for key in summary_mapping if isinstance(key, str)}
    if keys & _NEIGHBORHOOD_SUMMARY_HINTS:
        return "neighborhood"
    if keys & _RUN_SUMMARY_HINTS:
        return "run"
    if keys & _IMPACT_SUMMARY_HINTS:
        return "impact"
    if keys & _CALLS_SUMMARY_HINTS:
        return "calls"
    return resolve_summary_variant_name(
        mode=summary_mapping.get("mode"),
        explicit=summary_mapping.get("summary_variant"),
        macro=macro,
    )


def _normalize_summary_mapping(
    summary_mapping: Mapping[str, object],
    *,
    macro: str | None,
) -> Mapping[str, object]:
    variant = _infer_summary_variant(summary_mapping, macro=macro)
    allowed_fields = set(summary_class_for_variant(variant).__struct_fields__)
    normalized = {
        key: value
        for key, value in summary_mapping.items()
        if isinstance(key, str) and key in allowed_fields
    }
    normalized["summary_variant"] = variant
    return cast("Mapping[str, object]", normalized)


def _thaw_summary_container(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _thaw_summary_container(item)
            for key, item in value.items()
            if isinstance(key, str)
        }
    if isinstance(value, tuple):
        return [_thaw_summary_container(item) for item in value]
    if isinstance(value, list):
        return [_thaw_summary_container(item) for item in value]
    return value


def _thaw_summary_for_decoded_result(summary: object) -> object:
    if not isinstance(summary, msgspec.Struct):
        return summary
    fields = getattr(summary, "__struct_fields__", ())
    if not isinstance(fields, tuple):
        return summary
    updates: dict[str, object] = {}
    for field_name in fields:
        value = getattr(summary, field_name)
        thawed = _thaw_summary_container(value)
        if field_name in {"steps", "language_order"} and isinstance(thawed, list) and not thawed:
            thawed = None
        if thawed is not value:
            updates[field_name] = thawed
    return msgspec.structs.replace(summary, **updates) if updates else summary


JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(strict=True)
JSON_RESULT_DECODER = msgspec.json.Decoder(type=object, strict=True)
MSGPACK_ENCODER = msgspec.msgpack.Encoder()
MSGPACK_DECODER = msgspec.msgpack.Decoder(type=object)
MSGPACK_RESULT_DECODER = msgspec.msgpack.Decoder(type=object)


def _to_contract_builtins_recursive(value: object) -> object:
    """Normalize payloads into deterministic builtins for wire encoding.

    Returns:
        object: Builtins payload safe for JSON/msgpack encoding.
    """
    normalized: object
    if isinstance(value, msgspec.Struct):
        normalized = _to_contract_builtins_recursive(msgspec.structs.asdict(value))
    elif isinstance(value, Mapping):
        normalized_items = sorted(
            ((str(key), _to_contract_builtins_recursive(item)) for key, item in value.items()),
            key=lambda item: item[0],
        )
        normalized = dict(normalized_items)
    elif isinstance(value, (list, tuple)):
        normalized = [_to_contract_builtins_recursive(item) for item in value]
    elif isinstance(value, (set, frozenset)):
        normalized = [_to_contract_builtins_recursive(item) for item in value]
        normalized.sort(key=repr)
    elif isinstance(value, bytes | bytearray | memoryview):
        normalized = bytes(value)
    elif value is None or isinstance(value, str | int | float | bool):
        normalized = value
    else:
        normalized = msgspec.to_builtins(value, order="deterministic", str_keys=True)
    return normalized


def _decode_result_payload(payload: object) -> CqResult:
    if not isinstance(payload, Mapping):
        msg = f"Expected CqResult payload mapping, got {type(payload).__name__}"
        raise TypeError(msg)

    run = msgspec.convert(payload.get("run"), type=RunMeta, strict=True)
    key_findings = msgspec.convert(
        payload.get("key_findings", ()),
        type=tuple[Finding, ...],
        strict=True,
    )
    evidence = msgspec.convert(
        payload.get("evidence", ()),
        type=tuple[Finding, ...],
        strict=True,
    )
    sections = msgspec.convert(
        payload.get("sections", ()),
        type=tuple[Section, ...],
        strict=True,
    )
    artifacts = msgspec.convert(
        payload.get("artifacts", ()),
        type=tuple[Artifact, ...],
        strict=True,
    )

    summary_raw = payload.get("summary")
    if summary_raw is None:
        summary = summary_from_mapping(None)
    elif isinstance(summary_raw, Mapping):
        summary = summary_from_mapping(
            _normalize_summary_mapping(
                cast("Mapping[str, object]", summary_raw),
                macro=run.macro if isinstance(run.macro, str) else None,
            )
        )
    else:
        msg = f"Expected summary mapping, got {type(summary_raw).__name__}"
        raise TypeError(msg)
    summary = cast("SummaryV1", _thaw_summary_for_decoded_result(summary))

    return CqResult(
        run=run,
        summary=summary,
        key_findings=key_findings,
        evidence=evidence,
        sections=sections,
        artifacts=artifacts,
    )


def encode_json(value: object, *, indent: int | None = None) -> str:
    """Encode any contract payload to deterministic JSON.

    Returns:
        str: Function return value.
    """
    payload = JSON_ENCODER.encode(to_contract_builtins(value))
    if indent is None:
        return payload.decode("utf-8")
    return msgspec.json.format(payload, indent=indent).decode("utf-8")


def decode_json(payload: bytes | str) -> object:
    """Decode JSON payload to builtins value.

    Returns:
        object: Function return value.
    """
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return JSON_DECODER.decode(payload)


def decode_json_result(payload: bytes | str) -> CqResult:
    """Decode JSON payload to typed CQ result.

    Returns:
        CqResult: Function return value.
    """
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return _decode_result_payload(JSON_RESULT_DECODER.decode(payload))


def encode_msgpack(value: object) -> bytes:
    """Encode payload to msgpack bytes.

    Returns:
        bytes: Function return value.
    """
    return MSGPACK_ENCODER.encode(to_contract_builtins(value))


def decode_msgpack(payload: bytes | bytearray | memoryview) -> object:
    """Decode msgpack payload to builtins value.

    Returns:
        object: Function return value.
    """
    return MSGPACK_DECODER.decode(payload)


def decode_msgpack_result(payload: bytes | bytearray | memoryview) -> CqResult:
    """Decode msgpack payload to typed CQ result.

    Returns:
        CqResult: Function return value.
    """
    return _decode_result_payload(MSGPACK_RESULT_DECODER.decode(payload))


def to_contract_builtins(value: object) -> object:
    """Convert a CQ value to builtins with deterministic contract settings.

    Returns:
        object: Function return value.
    """
    return _to_contract_builtins_recursive(value)


def to_public_dict(value: msgspec.Struct) -> dict[str, object]:
    """Convert one msgspec Struct into mapping payload.

    Returns:
        dict[str, object]: Function return value.

    Raises:
        TypeError: Raised when the payload is not map-like.
    """
    payload = to_contract_builtins(value)
    if isinstance(payload, dict):
        return cast("dict[str, object]", payload)
    msg = f"Expected dict payload, got {type(payload).__name__}"
    raise TypeError(msg)


def to_public_list(values: Iterable[msgspec.Struct]) -> list[dict[str, object]]:
    """Convert iterable of structs into mapping rows.

    Returns:
        list[dict[str, object]]: Function return value.
    """
    return [to_public_dict(value) for value in values]


def require_mapping(value: object) -> dict[str, object]:
    """Require mapping-shaped builtins payload.

    Returns:
        dict[str, object]: Function return value.

    Raises:
        TypeError: Raised when the payload cannot be represented as a mapping.
    """
    payload = to_contract_builtins(value)
    if isinstance(payload, dict):
        enforce_mapping_constraints(payload)
        return cast("dict[str, object]", payload)
    msg = f"Expected mapping payload, got {type(payload).__name__}"
    raise TypeError(msg)


__all__ = [
    "JSON_DECODER",
    "JSON_ENCODER",
    "JSON_RESULT_DECODER",
    "MSGPACK_DECODER",
    "MSGPACK_ENCODER",
    "MSGPACK_RESULT_DECODER",
    "decode_json",
    "decode_json_result",
    "decode_msgpack",
    "decode_msgpack_result",
    "encode_json",
    "encode_msgpack",
    "require_mapping",
    "to_contract_builtins",
    "to_public_dict",
    "to_public_list",
]
