"""Payload budget helpers for Python enrichment."""

from __future__ import annotations

from tools.cq.search.enrichment.core import (
    enforce_payload_budget as enforce_shared_payload_budget,
)
from tools.cq.search.enrichment.core import payload_size_hint as shared_payload_size_hint

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


__all__ = ["enforce_payload_budget", "payload_size_hint"]


def payload_size_hint(payload: dict[str, object]) -> int:
    """Estimate encoded payload size in bytes.

    Returns:
        int: Estimated encoded payload size.
    """
    return shared_payload_size_hint(payload)


def enforce_payload_budget(
    payload: dict[str, object],
    *,
    max_payload_bytes: int,
    drop_order: tuple[str, ...] = _DEFAULT_DROP_ORDER,
) -> tuple[list[str], int]:
    """Prune optional fields when payload exceeds max size.

    Returns:
        tuple[list[str], int]: Removed field names and final payload size estimate.
    """
    return enforce_shared_payload_budget(
        payload,
        max_payload_bytes=max_payload_bytes,
        drop_order=drop_order,
    )
