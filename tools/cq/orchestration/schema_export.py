"""Schema export helpers for cq msgspec models."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import msgspec

from tools.cq.core.locations import SourceSpan
from tools.cq.core.schema import Artifact, Finding, RunMeta, Section
from tools.cq.core.scoring import ConfidenceSignals, ImpactSignals
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.ir import Query
from tools.cq.run.spec import RunPlan, RunStep
from tools.cq.search._shared.search_contracts import SearchSummaryContract
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.tree_sitter.contracts.core_models import (
    TreeSitterArtifactBundleV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)


class CqResultSchemaV1(msgspec.Struct, omit_defaults=True):
    """Schema-export-safe projection of :class:`tools.cq.core.schema.CqResult`."""

    run: RunMeta
    summary: dict[str, object] = msgspec.field(default_factory=dict)
    key_findings: list[Finding] = msgspec.field(default_factory=list)
    evidence: list[Finding] = msgspec.field(default_factory=list)
    sections: list[Section] = msgspec.field(default_factory=list)
    artifacts: list[Artifact] = msgspec.field(default_factory=list)


def _alias_schema_type(
    *,
    schema: dict[str, Any],
    source_name: str,
    alias_name: str,
) -> dict[str, Any]:
    defs = schema.get("$defs")
    if not isinstance(defs, dict) or source_name not in defs:
        return schema
    if alias_name not in defs:
        defs[alias_name] = defs[source_name]
    ref = schema.get("$ref")
    expected_ref = f"#/$defs/{source_name}"
    if isinstance(ref, str) and ref == expected_ref:
        schema["$ref"] = f"#/$defs/{alias_name}"
    return schema


def _alias_component_type(
    *,
    schema_rows: tuple[dict[str, Any], ...],
    components: dict[str, Any],
    source_name: str,
    alias_name: str,
) -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
    if source_name in components and alias_name not in components:
        components[alias_name] = components[source_name]

    source_ref = f"#/$defs/{source_name}"
    alias_ref = f"#/$defs/{alias_name}"
    aliased_rows: list[dict[str, Any]] = []
    for row in schema_rows:
        row_copy = dict(row)
        if row_copy.get("$ref") == source_ref:
            row_copy["$ref"] = alias_ref
        aliased_rows.append(row_copy)
    return tuple(aliased_rows), components


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
    schema = msgspec.json.schema(CqResultSchemaV1, schema_hook=cast("Any", _schema_hook))
    return _alias_schema_type(
        schema=schema,
        source_name="CqResultSchemaV1",
        alias_name="CqResult",
    )


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
            CqResultSchemaV1,
            Query,
            RunPlan,
            RunStep,
            SearchSummaryContract,
            TreeSitterArtifactBundleV1,
            TreeSitterDiagnosticV1,
            TreeSitterQueryHitV1,
            SearchLimits,
            Toolchain,
            ImpactSignals,
            ConfidenceSignals,
            SourceSpan,
            SearchConfig,
        ],
        schema_hook=cast("Any", _schema_hook),
    )
    return _alias_component_type(
        schema_rows=schema,
        components=components,
        source_name="CqResultSchemaV1",
        alias_name="CqResult",
    )


__all__ = [
    "cq_result_schema",
    "cq_schema_components",
    "query_schema",
]
