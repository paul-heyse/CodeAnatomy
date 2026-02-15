"""Runtime grammar-id helpers."""

from __future__ import annotations

from typing import Protocol


class RuntimeLanguage(Protocol):
    """Subset of tree-sitter Language APIs used for runtime id lookups."""

    def id_for_node_kind(self, name: str, named: object) -> int:
        """Return node-kind id for a given name."""
        ...

    def field_id_for_name(self, name: str) -> int:
        """Return field id for a given field name."""
        ...


def build_runtime_ids(language: RuntimeLanguage) -> dict[str, int]:
    """Build runtime node-kind ids for frequently queried symbols."""
    out: dict[str, int] = {}
    named = True
    for kind in ("identifier", "call", "function_definition"):
        try:
            out[kind] = int(language.id_for_node_kind(kind, named))
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
    return out


def build_runtime_field_ids(language: RuntimeLanguage) -> dict[str, int]:
    """Build runtime field ids for frequently queried field names."""
    out: dict[str, int] = {}
    for field_name in ("name", "body", "parameters"):
        try:
            out[field_name] = int(language.field_id_for_name(field_name))
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
    return out


__all__ = ["build_runtime_field_ids", "build_runtime_ids"]
