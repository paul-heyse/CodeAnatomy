"""Payload budget helpers for Python enrichment."""

from __future__ import annotations

from tools.cq.search.enrichment.core import (
    check_payload_budget as check_shared_payload_budget,
)
from tools.cq.search.enrichment.core import payload_size_hint as shared_payload_size_hint
from tools.cq.search.enrichment.core import trim_payload_to_budget as trim_shared_payload_to_budget

_DEFAULT_DROP_ORDER: tuple[str, ...] = (
    "scope_chain",
    "decorators",
    "base_classes",
    "property_names",
    "import_names",
    "signature",
    "call_target",
    "structural_context",
)


__all__ = ["check_payload_budget", "payload_size_hint", "trim_payload_to_budget"]


def payload_size_hint(payload: dict[str, object]) -> int:
    """Estimate encoded payload size in bytes.

    Returns:
        int: Estimated encoded payload size.
    """
    return shared_payload_size_hint(payload)


def check_payload_budget(
    payload: dict[str, object],
    *,
    max_payload_bytes: int,
) -> tuple[bool, int]:
    """Check if payload already fits the configured size budget.

    Returns:
        tuple[bool, int]: ``(fits_budget, size_hint)``.
    """
    return check_shared_payload_budget(payload, max_payload_bytes=max_payload_bytes)


def trim_payload_to_budget(
    payload: dict[str, object],
    *,
    max_payload_bytes: int,
    drop_order: tuple[str, ...] = _DEFAULT_DROP_ORDER,
) -> tuple[dict[str, object], list[str], int]:
    """Return trimmed payload copy when payload exceeds max size.

    Returns:
        tuple[dict[str, object], list[str], int]:
            Trimmed payload, removed field names, and final payload size estimate.
    """
    return trim_shared_payload_to_budget(
        payload,
        max_payload_bytes=max_payload_bytes,
        drop_order=drop_order,
    )
