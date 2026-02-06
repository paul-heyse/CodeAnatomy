"""Central extract metadata definitions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache

from datafusion_engine.extract.templates import dataset_template_specs, expand_dataset_templates
from utils.value_coercion import raise_for_int


@dataclass(frozen=True)
class ExtractDerivedIdSpec:
    """Derived identifier specification for extract datasets."""

    name: str
    spec: str
    kind: str = "masked_hash"
    required: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExtractOrderingKeySpec:
    """Ordering key specification for extract datasets."""

    column: str
    order: str = "ascending"


@dataclass(frozen=True)
class ExtractMetadata:
    """Central extract metadata for a dataset."""

    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    derived: tuple[ExtractDerivedIdSpec, ...] = ()
    row_fields: tuple[str, ...] = ()
    row_extras: tuple[str, ...] = ()
    template: str | None = None
    ordering_keys: tuple[ExtractOrderingKeySpec, ...] = ()
    join_keys: tuple[str, ...] = ()
    enabled_when: str | None = None
    feature_flag: str | None = None
    postprocess: str | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    evidence_required_columns: tuple[str, ...] = ()
    pipeline_name: str | None = None

    def output_name(self) -> str:
        """Return the canonical output alias for the dataset row.

        Returns:
        -------
        str
            Output alias used by extract bundles.
        """
        suffix = f"_v{self.version}"
        base = self.name[: -len(suffix)] if self.name.endswith(suffix) else self.name
        if self.template in {"ast", "cst"} and base.startswith("py_"):
            return base[3:]
        return base


def _string_tuple(value: object | None, *, label: str) -> tuple[str, ...]:
    if not value:
        return ()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value)
    msg = f"{label} must be a sequence of strings."
    raise TypeError(msg)


def _derived_specs(value: object | None) -> tuple[ExtractDerivedIdSpec, ...]:
    if not value:
        return ()
    if not isinstance(value, Sequence):
        msg = "derived specs must be a sequence of mappings."
        raise TypeError(msg)
    items: list[ExtractDerivedIdSpec] = []
    for item in value:
        if not isinstance(item, Mapping):
            msg = "derived spec entries must be mappings."
            raise TypeError(msg)
        items.append(
            ExtractDerivedIdSpec(
                name=str(item.get("name")),
                spec=str(item.get("spec")),
                kind=str(item.get("kind", "masked_hash")),
                required=_string_tuple(item.get("required"), label="derived.required"),
            )
        )
    return tuple(items)


def _ordering_specs(value: object | None) -> tuple[ExtractOrderingKeySpec, ...]:
    if not value:
        return ()
    if not isinstance(value, Sequence):
        msg = "ordering_keys must be a sequence of mappings."
        raise TypeError(msg)
    items: list[ExtractOrderingKeySpec] = []
    for item in value:
        if not isinstance(item, Mapping):
            msg = "ordering key entries must be mappings."
            raise TypeError(msg)
        items.append(
            ExtractOrderingKeySpec(
                column=str(item.get("column")),
                order=str(item.get("order", "ascending")),
            )
        )
    return tuple(items)


def _to_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def _metadata_extra(value: object | None) -> dict[bytes, bytes]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {_to_bytes(key): _to_bytes(val) for key, val in value.items()}
    msg = "metadata_extra must be a mapping of strings."
    raise TypeError(msg)


def _metadata_from_record(record: Mapping[str, object]) -> ExtractMetadata:
    return ExtractMetadata(
        name=str(record.get("name")),
        version=raise_for_int(record.get("version"), context="version"),
        bundles=_string_tuple(record.get("bundles"), label="bundles"),
        fields=_string_tuple(record.get("fields"), label="fields"),
        derived=_derived_specs(record.get("derived")),
        row_fields=_string_tuple(record.get("row_fields"), label="row_fields"),
        row_extras=_string_tuple(record.get("row_extras"), label="row_extras"),
        template=str(record.get("template")) if record.get("template") else None,
        ordering_keys=_ordering_specs(record.get("ordering_keys")),
        join_keys=_string_tuple(record.get("join_keys"), label="join_keys"),
        enabled_when=str(record.get("enabled_when")) if record.get("enabled_when") else None,
        feature_flag=str(record.get("feature_flag")) if record.get("feature_flag") else None,
        postprocess=str(record.get("postprocess")) if record.get("postprocess") else None,
        metadata_extra=_metadata_extra(record.get("metadata_extra")),
        evidence_required_columns=_string_tuple(
            record.get("evidence_required_columns"),
            label="evidence_required_columns",
        ),
        pipeline_name=str(record.get("pipeline_name")) if record.get("pipeline_name") else None,
    )


@cache
def extract_metadata_specs() -> tuple[ExtractMetadata, ...]:
    """Return the extract metadata specs derived from template expansion.

    Returns:
    -------
    tuple[ExtractMetadata, ...]
        Extract metadata records in registry order.
    """
    records = expand_dataset_templates(dataset_template_specs())
    return tuple(_metadata_from_record(record) for record in records)


@cache
def extract_metadata_by_name() -> Mapping[str, ExtractMetadata]:
    """Return extract metadata mapped by dataset name.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    metadata: dict[str, ExtractMetadata] = {}
    for spec in extract_metadata_specs():
        if spec.name in metadata:
            msg = f"Duplicate extract metadata name: {spec.name!r}."
            raise ValueError(msg)
        metadata[spec.name] = spec
    return metadata


__all__ = [
    "ExtractDerivedIdSpec",
    "ExtractMetadata",
    "ExtractOrderingKeySpec",
    "extract_metadata_by_name",
    "extract_metadata_specs",
]
