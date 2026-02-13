"""Shared JSON-compatible type aliases for CQ public payloads."""

from __future__ import annotations

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]

__all__ = ["JsonScalar", "JsonValue"]
