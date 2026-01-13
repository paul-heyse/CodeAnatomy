"""Registry validation helpers for normalize rules."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from arrowdsl.core.interop import SchemaLike
from normalize.contracts import normalize_evidence_schema
from normalize.evidence_specs import evidence_output_from_schema, evidence_spec_from_schema
from normalize.registry_specs import dataset_names, dataset_schema
from normalize.rule_model import AmbiguityPolicy, EvidenceOutput, EvidenceSpec, NormalizeRule

if TYPE_CHECKING:
    from arrowdsl.compute.expr_core import ScalarValue


def validate_rule_specs(rules: Sequence[NormalizeRule]) -> None:
    """Validate normalize rule specs against registry contracts."""
    registered = set(dataset_names())
    for rule in rules:
        output_schema = dataset_schema(rule.output)
        _validate_evidence_output(rule, output_schema)
        evidence = _merge_evidence(rule.evidence, evidence_spec_from_schema(output_schema))
        _validate_evidence_requirements(rule, evidence, registered=registered)
        _validate_ambiguity_policy(rule.name, rule.ambiguity_policy)


def _validate_evidence_output(rule: NormalizeRule, schema: SchemaLike) -> None:
    evidence_output = _merge_evidence_output(
        rule.evidence_output, evidence_output_from_schema(schema)
    )
    if evidence_output is None:
        return
    evidence_schema = normalize_evidence_schema()
    evidence_columns = set(evidence_schema.names)
    output_columns = set(schema.names)
    missing_targets = sorted(
        {value for value in evidence_output.column_map.values() if value not in output_columns}
    )
    if missing_targets:
        msg = (
            "Normalize rule evidence output references missing columns "
            f"for rule {rule.name!r}: {missing_targets}"
        )
        raise ValueError(msg)
    missing_keys = sorted(
        name for name in evidence_output.column_map if name not in evidence_columns
    )
    missing_literals = sorted(
        name for name in evidence_output.literals if name not in evidence_columns
    )
    if missing_keys or missing_literals:
        msg = (
            "Normalize rule evidence output uses unknown evidence columns "
            f"for rule {rule.name!r}: {sorted(set(missing_keys + missing_literals))}"
        )
        raise ValueError(msg)


def _validate_evidence_requirements(
    rule: NormalizeRule,
    evidence: EvidenceSpec | None,
    *,
    registered: set[str],
) -> None:
    if evidence is None:
        return
    sources = evidence.sources or rule.inputs
    for source in sources:
        if source not in registered:
            continue
        schema = dataset_schema(source)
        _validate_required_columns(rule, source, schema, evidence.required_columns)
        _validate_required_types(rule, source, schema, evidence.required_types)
        _validate_required_metadata(rule, source, schema, evidence.required_metadata)


def _validate_required_columns(
    rule: NormalizeRule,
    source: str,
    schema: SchemaLike,
    required: Sequence[str],
) -> None:
    if not required:
        return
    missing = sorted(set(required) - set(schema.names))
    if missing:
        msg = (
            "Normalize rule evidence requirements reference missing columns "
            f"for rule {rule.name!r} source {source!r}: {missing}"
        )
        raise ValueError(msg)


def _validate_required_types(
    rule: NormalizeRule,
    source: str,
    schema: SchemaLike,
    required: Mapping[str, str],
) -> None:
    if not required:
        return
    types = {field.name: str(field.type) for field in schema}
    missing = sorted(name for name in required if name not in types)
    if missing:
        msg = (
            "Normalize rule evidence requirements reference missing type columns "
            f"for rule {rule.name!r} source {source!r}: {missing}"
        )
        raise ValueError(msg)
    mismatched = sorted(name for name, dtype in required.items() if types.get(name) != dtype)
    if mismatched:
        msg = (
            "Normalize rule evidence requirements include mismatched types "
            f"for rule {rule.name!r} source {source!r}: {mismatched}"
        )
        raise ValueError(msg)


def _validate_required_metadata(
    rule: NormalizeRule,
    source: str,
    schema: SchemaLike,
    required: Mapping[bytes, bytes],
) -> None:
    if not required:
        return
    metadata = schema.metadata or {}
    missing = sorted(key.decode("utf-8") for key in required if key not in metadata)
    if missing:
        msg = (
            "Normalize rule evidence requirements reference missing metadata "
            f"for rule {rule.name!r} source {source!r}: {missing}"
        )
        raise ValueError(msg)
    mismatched = [
        key.decode("utf-8") for key, value in required.items() if metadata.get(key) != value
    ]
    if mismatched:
        msg = (
            "Normalize rule evidence requirements include mismatched metadata "
            f"for rule {rule.name!r} source {source!r}: {sorted(mismatched)}"
        )
        raise ValueError(msg)


def _validate_ambiguity_policy(rule_name: str, policy: AmbiguityPolicy | None) -> None:
    if policy is None or policy.winner_select is None:
        return
    schema = normalize_evidence_schema()
    available = set(schema.names)
    required: set[str] = set(policy.winner_select.keys)
    required.add(policy.winner_select.score_col)
    required.update(key.column for key in policy.winner_select.tie_breakers)
    required.update(key.column for key in policy.tie_breakers)
    missing = sorted(required - available)
    if missing:
        msg = (
            "Normalize ambiguity policy references missing evidence columns "
            f"for rule {rule_name!r}: {missing}"
        )
        raise ValueError(msg)


def _merge_evidence(
    base: EvidenceSpec | None,
    defaults: EvidenceSpec | None,
) -> EvidenceSpec | None:
    if base is None:
        return defaults
    if defaults is None:
        return base
    sources = base.sources or defaults.sources
    required_columns = tuple(sorted(set(base.required_columns).union(defaults.required_columns)))
    required_types = dict(defaults.required_types)
    required_types.update(base.required_types)
    required_metadata = dict(defaults.required_metadata)
    required_metadata.update(base.required_metadata)
    if not sources and not required_columns and not required_types and not required_metadata:
        return None
    return EvidenceSpec(
        sources=sources,
        required_columns=required_columns,
        required_types=required_types,
        required_metadata=required_metadata,
    )


def _merge_evidence_output(
    base: EvidenceOutput | None,
    defaults: EvidenceOutput | None,
) -> EvidenceOutput | None:
    if base is None:
        return defaults
    if defaults is None:
        return base
    column_map = dict(defaults.column_map)
    column_map.update(base.column_map)
    literals: dict[str, ScalarValue] = dict(defaults.literals)
    literals.update(base.literals)
    if not column_map and not literals:
        return None
    return EvidenceOutput(column_map=column_map, literals=literals)


__all__ = ["validate_rule_specs"]
