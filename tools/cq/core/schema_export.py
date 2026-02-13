"""Schema export helpers for cq msgspec models."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import msgspec

from tools.cq.core.locations import SourceSpan
from tools.cq.core.schema import CqResult
from tools.cq.core.scoring import ConfidenceSignals, ImpactSignals
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.ir import Query
from tools.cq.search.models import SearchConfig
from tools.cq.search.profiles import SearchLimits


def _schema_hook(type_: object) -> dict[str, Any] | None:
    """Provide schema fallback for dynamic object-typed fields.

    Msgspec schema export rejects ``object`` without a hook. CQ result contracts
    still expose a few dynamic payload slots by design.

    Returns:
    -------
    dict[str, Any] | None
        JSON Schema override for the requested type.
    """
    if type_ is object:
        return {
            "anyOf": [
                {"type": "string"},
                {"type": "number"},
                {"type": "integer"},
                {"type": "boolean"},
                {"type": "null"},
                {"type": "array"},
                {"type": "object"},
            ]
        }
    if isinstance(type_, type) and issubclass(type_, Path):
        return {"type": "string"}
    return None


def cq_result_schema() -> dict[str, Any]:
    """Return JSON Schema for CQ result output.

    Returns:
    -------
    dict[str, Any]
        JSON Schema for the result model.
    """
    return msgspec.json.schema(CqResult, schema_hook=cast("Any", _schema_hook))


def query_schema() -> dict[str, Any]:
    """Return JSON Schema for CQ query IR.

    Returns:
    -------
    dict[str, Any]
        JSON Schema for the query model.
    """
    return msgspec.json.schema(Query, schema_hook=cast("Any", _schema_hook))


def cq_schema_components() -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
    """Return schema and components for CQ result + query IR.

    Returns:
    -------
    tuple[dict[str, Any], dict[str, Any]]
        Schema and components sections.
    """
    schema, components = msgspec.json.schema_components(
        [
            CqResult,
            Query,
            SearchLimits,
            Toolchain,
            ImpactSignals,
            ConfidenceSignals,
            SourceSpan,
            SearchConfig,
        ],
        schema_hook=cast("Any", _schema_hook),
    )
    return schema, components


__all__ = [
    "cq_result_schema",
    "cq_schema_components",
    "query_schema",
]
