"""Helpers enforcing UUID temporal contracts for CQ run/event identity."""

from __future__ import annotations

import threading
import uuid
from typing import cast

from tools.cq.utils.uuid_factory import (
    UUID6_MODULE,
    legacy_compatible_event_id,
    normalize_legacy_identity,
    uuid7,
)
from tools.cq.utils.uuid_temporal_contracts_models import RunIdentityContractV1, TemporalUuidInfoV1

_UUID_ALT_LOCK = threading.Lock()
_UUID7_VERSION = 7


def _variant_name(variant: object) -> str:
    if variant == uuid.RFC_4122:
        return "rfc_4122"
    if variant == uuid.RESERVED_NCS:
        return "reserved_ncs"
    if variant == uuid.RESERVED_MICROSOFT:
        return "reserved_microsoft"
    if variant == uuid.RESERVED_FUTURE:
        return "reserved_future"
    return "unknown"


def uuid_time_millis(value: uuid.UUID) -> int | None:
    """Return epoch milliseconds for UUIDv7 values."""
    if value.version != _UUID7_VERSION:
        return None
    raw_time = getattr(value, "time", None)
    if not isinstance(raw_time, int):
        return None
    return int(raw_time)


def temporal_uuid_info(value: uuid.UUID) -> TemporalUuidInfoV1:
    """Return normalized metadata for any UUID."""
    return TemporalUuidInfoV1(
        value=str(value),
        version=int(value.version or 0),
        variant=_variant_name(value.variant),
        time_ms=uuid_time_millis(value),
    )


def normalize_external_identity(raw_value: str | uuid.UUID) -> uuid.UUID:
    """Normalize external UUID values into CQ's canonical identity form.

    Returns:
        uuid.UUID: Canonicalized UUID identity.
    """
    incoming = raw_value if isinstance(raw_value, uuid.UUID) else uuid.UUID(str(raw_value))
    if incoming.version == 1 and UUID6_MODULE is not None:
        convert = getattr(UUID6_MODULE, "uuid1_to_uuid6", None)
        if callable(convert):
            return cast("uuid.UUID", convert(incoming))
    return normalize_legacy_identity(incoming)


def gated_uuid8(*, enable_v8: bool) -> uuid.UUID:
    """Return UUIDv8 only when enabled and available; fallback to UUIDv7.

    Returns:
        uuid.UUID: Generated UUID value.
    """
    if not enable_v8 or UUID6_MODULE is None:
        return uuid7()
    uuid8_fn = getattr(UUID6_MODULE, "uuid8", None)
    if not callable(uuid8_fn):
        return uuid7()
    with _UUID_ALT_LOCK:
        return cast("uuid.UUID", uuid8_fn())


def legacy_event_uuid(*, node: int | None = None, clock_seq: int | None = None) -> uuid.UUID:
    """Return a legacy-compatible event UUID (v6 when available).

    Returns:
        uuid.UUID: Generated event UUID.
    """
    with _UUID_ALT_LOCK:
        return legacy_compatible_event_id(node=node, clock_seq=clock_seq)


def resolve_run_identity_contract(run_id: str | None = None) -> RunIdentityContractV1:
    """Resolve run identity contract from a provided or generated UUID.

    Returns:
        RunIdentityContractV1: Normalized run identity contract.
    """
    if isinstance(run_id, str) and run_id.strip():
        raw = run_id.strip()
        try:
            normalized = normalize_external_identity(raw)
        except (ValueError, TypeError, AttributeError):
            return RunIdentityContractV1(
                run_id=raw,
                run_uuid_version=0,
                run_variant="unknown",
                run_created_ms=0,
            )
    else:
        normalized = uuid7()
    run_time_ms = uuid_time_millis(normalized)
    if run_time_ms is None:
        run_time_ms = 0
    return RunIdentityContractV1(
        run_id=str(normalized),
        run_uuid_version=int(normalized.version or 0),
        run_variant=_variant_name(normalized.variant),
        run_created_ms=run_time_ms,
    )


__all__ = [
    "gated_uuid8",
    "legacy_event_uuid",
    "normalize_external_identity",
    "resolve_run_identity_contract",
    "temporal_uuid_info",
    "uuid_time_millis",
]
