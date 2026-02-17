"""Fact accessor helpers."""

from __future__ import annotations

from tools.cq.core.enrichment_facts import additional_language_payload, has_fact_value

__all__ = ["get_additional_language_payload", "is_fact_value_present"]


def is_fact_value_present(value: object) -> bool:
    """Return whether a fact value should be rendered."""
    return has_fact_value(value)


def get_additional_language_payload(payload: dict[str, object] | None) -> dict[str, object]:
    """Return additional-language payload rows."""
    return additional_language_payload(payload)
