"""Schema export helpers for cq msgspec models."""

from __future__ import annotations

from typing import Any

import msgspec

from tools.cq.core.schema import CqResult
from tools.cq.query.ir import Query


def cq_result_schema() -> dict[str, Any]:
    """Return JSON Schema for CQ result output."""
    return msgspec.json.schema(CqResult)


def query_schema() -> dict[str, Any]:
    """Return JSON Schema for CQ query IR."""
    return msgspec.json.schema(Query)


def cq_schema_components() -> tuple[dict[str, Any], dict[str, Any]]:
    """Return schema and components for CQ result + query IR."""
    schema, components = msgspec.json.schema_components([CqResult, Query])
    return schema, components


__all__ = [
    "cq_result_schema",
    "cq_schema_components",
    "query_schema",
]
