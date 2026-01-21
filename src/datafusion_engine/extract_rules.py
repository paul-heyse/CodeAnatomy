"""Extract rule definitions derived from DataFusion metadata."""

from __future__ import annotations

from functools import cache

from arrowdsl.core.ordering import OrderingKey
from datafusion_engine.extract_metadata import ExtractMetadata, extract_metadata_specs
from relspec.rules.definitions import ExtractPayload, RuleDefinition, RuleStage


def _ordering_keys(row: ExtractMetadata) -> tuple[OrderingKey, ...]:
    return tuple((item.column, item.order) for item in row.ordering_keys)


def _extract_stages(payload: ExtractPayload) -> tuple[RuleStage, ...]:
    enabled_when = _stage_enabled_when(payload)
    stages = [RuleStage(name="source", mode="source", enabled_when=enabled_when)]
    if payload.postprocess:
        stages.append(RuleStage(name=payload.postprocess, mode="post_kernel"))
    return tuple(stages)


def _stage_enabled_when(payload: ExtractPayload) -> str | None:
    if payload.enabled_when is None:
        return None
    if payload.enabled_when == "feature_flag" and payload.feature_flag:
        return f"feature_flag:{payload.feature_flag}"
    return payload.enabled_when


def _payload_from_metadata(row: ExtractMetadata) -> ExtractPayload:
    return ExtractPayload(
        version=row.version,
        template=row.template,
        bundles=row.bundles,
        fields=row.fields,
        derived_ids=row.derived,
        row_fields=row.row_fields,
        row_extras=row.row_extras,
        ordering_keys=_ordering_keys(row),
        join_keys=row.join_keys,
        enabled_when=row.enabled_when,
        feature_flag=row.feature_flag,
        postprocess=row.postprocess,
        metadata_extra=row.metadata_extra,
        evidence_required_columns=row.evidence_required_columns,
        pipeline_name=row.pipeline_name or row.template,
    )


def _rule_from_metadata(row: ExtractMetadata) -> RuleDefinition:
    payload = _payload_from_metadata(row)
    return RuleDefinition(
        name=row.name,
        domain="extract",
        kind="extract",
        inputs=(),
        output=row.name,
        execution_mode="external",
        priority=100,
        emit_rule_meta=False,
        payload=payload,
        stages=_extract_stages(payload),
    )


@cache
def extract_rule_definitions() -> tuple[RuleDefinition, ...]:
    """Return extract rule definitions in registry order.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Extract rule definitions derived from metadata.
    """
    return tuple(_rule_from_metadata(row) for row in extract_metadata_specs())


__all__ = ["extract_rule_definitions"]
