"""Dataset rows describing extract schemas and derivations."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

from arrowdsl.core.context import OrderingKey
from arrowdsl.spec.tables.extract import ExtractDatasetRowSpec
from extract.registry_tables import DATASET_ROW_SPECS
from extract.registry_templates import flag_default

DerivedKind = Literal["masked_hash", "hash"]


def _flag_enabled(flag: str, *, default: bool = True) -> Callable[[object | None], bool]:
    def _predicate(options: object | None) -> bool:
        resolved_default = flag_default(flag, fallback=default)
        if options is None:
            return resolved_default
        value = getattr(options, flag, resolved_default)
        return value if isinstance(value, bool) else resolved_default

    return _predicate


def _allowlist_enabled(options: object | None) -> bool:
    if options is None:
        return False
    allowlist = getattr(options, "module_allowlist", ())
    return bool(allowlist)


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
    enabled_when: Callable[[object | None], bool] | None = None
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


def _enabled_predicate(
    spec: ExtractDatasetRowSpec,
) -> Callable[[object | None], bool] | None:
    if spec.enabled_when is None:
        return None
    if spec.enabled_when == "feature_flag":
        if spec.feature_flag is None:
            msg = f"Dataset {spec.name} enabled_when requires a feature_flag."
            raise ValueError(msg)
        return _flag_enabled(spec.feature_flag)
    if spec.enabled_when == "allowlist":
        return _allowlist_enabled
    msg = f"Unsupported enabled_when value: {spec.enabled_when!r}."
    raise ValueError(msg)


def _metadata_bytes(metadata: dict[str, str] | None) -> dict[bytes, bytes]:
    if not metadata:
        return {}
    return {key.encode("utf-8"): value.encode("utf-8") for key, value in metadata.items()}


def _derived_kind(value: str) -> DerivedKind:
    if value == "hash":
        return "hash"
    if value == "masked_hash":
        return "masked_hash"
    msg = f"Unsupported derived id kind: {value!r}."
    raise ValueError(msg)


def _derived_specs(spec: ExtractDatasetRowSpec) -> tuple[DerivedIdSpec, ...]:
    return tuple(
        DerivedIdSpec(
            name=item.name,
            spec=item.spec,
            kind=_derived_kind(item.kind),
            required=item.required,
        )
        for item in spec.derived
    )


def _ordering_keys(spec: ExtractDatasetRowSpec) -> tuple[OrderingKey, ...]:
    return tuple((item.column, item.order) for item in spec.ordering_keys)


def _dataset_row_from_spec(spec: ExtractDatasetRowSpec) -> DatasetRow:
    return DatasetRow(
        name=spec.name,
        version=spec.version,
        bundles=spec.bundles,
        fields=spec.fields,
        derived=_derived_specs(spec),
        row_fields=spec.row_fields,
        row_extras=spec.row_extras,
        template=spec.template,
        ordering_keys=_ordering_keys(spec),
        join_keys=spec.join_keys,
        enabled_when=_enabled_predicate(spec),
        feature_flag=spec.feature_flag,
        postprocess=spec.postprocess,
        metadata_extra=_metadata_bytes(spec.metadata_extra),
        evidence_required_columns=spec.evidence_required_columns,
    )


DATASET_ROWS: tuple[DatasetRow, ...] = tuple(
    _dataset_row_from_spec(spec) for spec in DATASET_ROW_SPECS
)


__all__ = ["DATASET_ROWS", "DatasetRow", "DerivedIdSpec"]
