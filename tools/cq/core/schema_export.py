"""Schema export helpers for cq msgspec models."""

from __future__ import annotations

from typing import Any

import msgspec

from tools.cq.core.locations import SourceSpan
from tools.cq.core.schema import CqResult
from tools.cq.core.scoring import ConfidenceSignals, ImpactSignals
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.ir import Query
from tools.cq.search.models import SearchConfig
from tools.cq.search.profiles import SearchLimits


def cq_result_schema() -> dict[str, Any]:
    """Return JSON Schema for CQ result output.

    Returns
    -------
    dict[str, Any]
        JSON Schema for the result model.
    """
    return msgspec.json.schema(CqResult)


def query_schema() -> dict[str, Any]:
    """Return JSON Schema for CQ query IR.

    Returns
    -------
    dict[str, Any]
        JSON Schema for the query model.
    """
    return msgspec.json.schema(Query)


def cq_schema_components() -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
    """Return schema and components for CQ result + query IR.

    Returns
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
        ]
    )
    return schema, components


__all__ = [
    "cq_result_schema",
    "cq_schema_components",
    "query_schema",
]
