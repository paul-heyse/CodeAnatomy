"""Evidence semantics registry for extract datasets."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.schema.metadata import EvidenceMetadata
from arrowdsl.schema.metadata import evidence_metadata_spec as _evidence_metadata_spec
from arrowdsl.schema.schema import SchemaMetadataSpec
from datafusion_engine.extract_metadata import ExtractMetadata, extract_metadata_specs
from datafusion_engine.extract_templates import template as template_lookup


@dataclass(frozen=True)
class EvidenceSpec:
    """Semantic metadata describing an evidence table."""

    name: str
    alias: str
    template: str | None
    evidence_family: str | None
    coordinate_system: str | None
    evidence_rank: int | None
    required_columns: tuple[str, ...]
    ambiguity_policy: str | None = None


def _template_value(row: ExtractMetadata, key: bytes) -> str | None:
    if row.template is None:
        return None
    templ = template_lookup(row.template)
    value = templ.metadata_extra.get(key)
    if value is None:
        return None
    try:
        return value.decode("utf-8")
    except UnicodeError:
        return None


def _required_columns(row: ExtractMetadata) -> tuple[str, ...]:
    if row.evidence_required_columns:
        return row.evidence_required_columns
    if row.join_keys:
        return row.join_keys
    return row.fields


def _build_spec(row: ExtractMetadata) -> EvidenceSpec:
    templ = template_lookup(row.template) if row.template is not None else None
    return EvidenceSpec(
        name=row.name,
        alias=row.output_name(),
        template=row.template,
        evidence_family=_template_value(row, b"evidence_family"),
        coordinate_system=_template_value(row, b"coordinate_system"),
        evidence_rank=templ.evidence_rank if templ is not None else None,
        required_columns=_required_columns(row),
        ambiguity_policy=_template_value(row, b"ambiguity_policy"),
    )


_SPECS_BY_NAME: dict[str, EvidenceSpec] = {}
_SPECS_BY_ALIAS: dict[str, EvidenceSpec] = {}
for _row in extract_metadata_specs():
    _spec = _build_spec(_row)
    _SPECS_BY_NAME[_spec.name] = _spec
    _SPECS_BY_ALIAS[_spec.alias] = _spec


def evidence_spec(name: str) -> EvidenceSpec:
    """Return the evidence spec for a dataset name or alias.

    Returns
    -------
    EvidenceSpec
        Evidence metadata spec for the dataset.

    Raises
    ------
    KeyError
        Raised when the dataset name or alias is unknown.
    """
    spec = _SPECS_BY_NAME.get(name) or _SPECS_BY_ALIAS.get(name)
    if spec is None:
        msg = f"Unknown evidence dataset: {name!r}."
        raise KeyError(msg)
    return spec


def evidence_specs() -> tuple[EvidenceSpec, ...]:
    """Return all evidence specs in registry order.

    Returns
    -------
    tuple[EvidenceSpec, ...]
        Evidence specs derived from registry rows.
    """
    return tuple(_SPECS_BY_NAME[name] for name in sorted(_SPECS_BY_NAME))


def evidence_metadata_spec(name: str) -> SchemaMetadataSpec:
    """Return schema metadata for evidence semantics.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec capturing evidence semantics.
    """
    spec = evidence_spec(name)
    labels: dict[str, str] = {"evidence_name": spec.name}
    if spec.evidence_family is not None:
        labels["evidence_family"] = spec.evidence_family
    if spec.coordinate_system is not None:
        labels["coordinate_system"] = spec.coordinate_system
    if spec.ambiguity_policy is not None:
        labels["ambiguity_policy"] = spec.ambiguity_policy
    if spec.template is not None:
        labels["evidence_template"] = spec.template
    metadata = EvidenceMetadata(
        labels=labels,
        required_columns=spec.required_columns,
        evidence_rank=spec.evidence_rank,
    )
    return _evidence_metadata_spec(metadata)


__all__ = ["EvidenceSpec", "evidence_metadata_spec", "evidence_spec", "evidence_specs"]
