"""Dataset rows describing extract schemas and derivations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from arrowdsl.core.context import OrderingKey
from extract.registry_definitions import extract_rule_definitions
from relspec.rules.definitions import ExtractPayload, RuleDefinition

DerivedKind = Literal["masked_hash", "hash"]


@dataclass(frozen=True)
class DerivedIdSpec:
    """Derived identifier specification for dataset rows."""

    name: str
    spec: str
    kind: DerivedKind = "masked_hash"
    required: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatasetRow:
    """Row spec describing a dataset schema and derivations."""

    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    derived: tuple[DerivedIdSpec, ...] = ()
    row_fields: tuple[str, ...] = ()
    row_extras: tuple[str, ...] = ()
    template: str | None = None
    ordering_keys: tuple[OrderingKey, ...] = ()
    join_keys: tuple[str, ...] = ()
    enabled_when: str | None = None
    feature_flag: str | None = None
    postprocess: str | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    evidence_required_columns: tuple[str, ...] = ()

    def output_name(self) -> str:
        """Return the canonical output alias for the dataset row.

        Returns
        -------
        str
            Output alias used by extract bundles.
        """
        suffix = f"_v{self.version}"
        base = self.name[: -len(suffix)] if self.name.endswith(suffix) else self.name
        if self.template in {"ast", "cst"} and base.startswith("py_"):
            return base[3:]
        return base


def _enabled_when_value(payload: ExtractPayload) -> str | None:
    if payload.enabled_when is None:
        return None
    if payload.enabled_when == "feature_flag" and payload.feature_flag:
        return f"feature_flag:{payload.feature_flag}"
    return payload.enabled_when


def _derived_kind(value: str) -> DerivedKind:
    if value == "hash":
        return "hash"
    if value == "masked_hash":
        return "masked_hash"
    msg = f"Unsupported derived id kind: {value!r}."
    raise ValueError(msg)


def _derived_specs(payload: ExtractPayload) -> tuple[DerivedIdSpec, ...]:
    return tuple(
        DerivedIdSpec(
            name=item.name,
            spec=item.spec,
            kind=_derived_kind(item.kind),
            required=item.required,
        )
        for item in payload.derived_ids
    )


def _ordering_keys(payload: ExtractPayload) -> tuple[OrderingKey, ...]:
    return tuple(payload.ordering_keys)


def _dataset_row_from_definition(defn: RuleDefinition) -> DatasetRow:
    if defn.domain != "extract":
        msg = f"RuleDefinition {defn.name!r} does not belong to extract."
        raise ValueError(msg)
    payload = defn.payload
    if not isinstance(payload, ExtractPayload):
        msg = f"RuleDefinition {defn.name!r} missing extract payload."
        raise TypeError(msg)
    return DatasetRow(
        name=defn.name,
        version=payload.version,
        bundles=payload.bundles,
        fields=payload.fields,
        derived=_derived_specs(payload),
        row_fields=payload.row_fields,
        row_extras=payload.row_extras,
        template=payload.template,
        ordering_keys=_ordering_keys(payload),
        join_keys=payload.join_keys,
        enabled_when=_enabled_when_value(payload),
        feature_flag=payload.feature_flag,
        postprocess=payload.postprocess,
        metadata_extra=dict(payload.metadata_extra),
        evidence_required_columns=payload.evidence_required_columns,
    )


DATASET_ROWS: tuple[DatasetRow, ...] = tuple(
    _dataset_row_from_definition(defn) for defn in extract_rule_definitions()
)


__all__ = ["DATASET_ROWS", "DatasetRow", "DerivedIdSpec"]
