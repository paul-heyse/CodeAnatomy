"""Central rule definitions for extract dataset registries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cache

from arrowdsl.core.context import OrderingKey
from extract.registry_template_specs import DATASET_TEMPLATE_SPECS
from extract.registry_templates import expand_dataset_templates
from extract.spec_tables import (
    ExtractDatasetRowSpec,
    ExtractDerivedIdSpec,
    dataset_rows_from_table,
    extract_dataset_table_from_rows,
)
from relspec.rules.definitions import ExtractPayload, RuleDefinition, RuleStage
from relspec.rules.validation import validate_rule_definitions

DATASET_ROW_RECORDS: tuple[Mapping[str, object], ...] = ()

_TEMPLATE_ROW_RECORDS = expand_dataset_templates(DATASET_TEMPLATE_SPECS)

EXTRACT_DATASET_TABLE = extract_dataset_table_from_rows(
    (*DATASET_ROW_RECORDS, *_TEMPLATE_ROW_RECORDS)
)


@cache
def dataset_row_specs() -> tuple[ExtractDatasetRowSpec, ...]:
    """Return extract dataset row specs in registry order.

    Returns
    -------
    tuple[ExtractDatasetRowSpec, ...]
        Dataset row specs derived from template expansion.
    """
    return dataset_rows_from_table(EXTRACT_DATASET_TABLE)


@cache
def extract_rule_definitions() -> tuple[RuleDefinition, ...]:
    """Return extract rule definitions in registry order.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Centralized rule definitions for extract datasets.
    """
    specs = dataset_row_specs()
    definitions = tuple(_rule_from_spec(spec) for spec in specs)
    _assert_roundtrip(specs, definitions)
    validate_rule_definitions(definitions)
    return definitions


def _rule_from_spec(spec: ExtractDatasetRowSpec) -> RuleDefinition:
    payload = ExtractPayload(
        version=spec.version,
        template=spec.template,
        bundles=spec.bundles,
        fields=spec.fields,
        derived_ids=_derived_specs(spec.derived),
        row_fields=spec.row_fields,
        row_extras=spec.row_extras,
        ordering_keys=_ordering_keys(spec),
        join_keys=spec.join_keys,
        enabled_when=spec.enabled_when,
        feature_flag=spec.feature_flag,
        postprocess=spec.postprocess,
        metadata_extra=_metadata_bytes(spec.metadata_extra),
        evidence_required_columns=spec.evidence_required_columns,
        pipeline_name=spec.pipeline_name or spec.template,
    )
    return RuleDefinition(
        name=spec.name,
        domain="extract",
        kind="extract",
        inputs=(),
        output=spec.name,
        execution_mode="external",
        priority=100,
        emit_rule_meta=False,
        payload=payload,
        stages=_extract_stages(payload),
    )


def _derived_specs(
    specs: Sequence[ExtractDerivedIdSpec],
) -> tuple[ExtractDerivedIdSpec, ...]:
    return tuple(specs)


def _ordering_keys(spec: ExtractDatasetRowSpec) -> tuple[OrderingKey, ...]:
    return tuple((item.column, item.order) for item in spec.ordering_keys)


def _metadata_bytes(metadata: dict[str, str] | None) -> dict[bytes, bytes]:
    if not metadata:
        return {}
    return {key.encode("utf-8"): value.encode("utf-8") for key, value in metadata.items()}


def _assert_roundtrip(
    specs: Sequence[ExtractDatasetRowSpec],
    definitions: Sequence[RuleDefinition],
) -> None:
    spec_map = {spec.name: spec for spec in specs}
    defn_names = {definition.name for definition in definitions}
    missing = sorted(name for name in spec_map if name not in defn_names)
    if missing:
        msg = f"Extract rule definitions missing dataset specs: {missing}"
        raise ValueError(msg)
    for definition in definitions:
        spec = spec_map.get(definition.name)
        if spec is None:
            continue
        payload = definition.payload
        if not isinstance(payload, ExtractPayload):
            msg = f"RuleDefinition {definition.name!r} missing extract payload."
            raise TypeError(msg)
        if not _payload_matches_spec(payload, spec):
            msg = f"Extract rule definition payload mismatch for {definition.name!r}."
            raise ValueError(msg)


def _extract_stages(payload: ExtractPayload) -> tuple[RuleStage, ...]:
    enabled_when = _stage_enabled_when(payload)
    stages = [RuleStage(name="source", mode="source", enabled_when=enabled_when)]
    if payload.postprocess:
        stages.append(RuleStage(name=payload.postprocess, mode="post_kernel"))
    return tuple(stages)


def _payload_matches_spec(payload: ExtractPayload, spec: ExtractDatasetRowSpec) -> bool:
    expected_pipeline = spec.pipeline_name or spec.template
    expected_metadata = _metadata_bytes(spec.metadata_extra)
    expected: dict[str, object] = {
        "version": spec.version,
        "template": spec.template,
        "bundles": spec.bundles,
        "fields": spec.fields,
        "derived_ids": spec.derived,
        "row_fields": spec.row_fields,
        "row_extras": spec.row_extras,
        "ordering_keys": _ordering_keys(spec),
        "join_keys": spec.join_keys,
        "enabled_when": spec.enabled_when,
        "feature_flag": spec.feature_flag,
        "postprocess": spec.postprocess,
        "metadata_extra": expected_metadata,
        "evidence_required_columns": spec.evidence_required_columns,
        "pipeline_name": expected_pipeline,
    }
    actual: dict[str, object] = {
        "version": payload.version,
        "template": payload.template,
        "bundles": payload.bundles,
        "fields": payload.fields,
        "derived_ids": payload.derived_ids,
        "row_fields": payload.row_fields,
        "row_extras": payload.row_extras,
        "ordering_keys": payload.ordering_keys,
        "join_keys": payload.join_keys,
        "enabled_when": payload.enabled_when,
        "feature_flag": payload.feature_flag,
        "postprocess": payload.postprocess,
        "metadata_extra": payload.metadata_extra,
        "evidence_required_columns": payload.evidence_required_columns,
        "pipeline_name": payload.pipeline_name,
    }
    return all(actual[key] == expected_value for key, expected_value in expected.items())


def _stage_enabled_when(payload: ExtractPayload) -> str | None:
    if payload.enabled_when is None:
        return None
    if payload.enabled_when == "feature_flag" and payload.feature_flag:
        return f"feature_flag:{payload.feature_flag}"
    return payload.enabled_when


__all__ = ["EXTRACT_DATASET_TABLE", "dataset_row_specs", "extract_rule_definitions"]
