"""Map property catalog entries to property field specs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING

from cpg.prop_catalog import PROP_SPECS, PropSpec
from cpg.specs import PropFieldSpec
from utils.validation import validate_required_items

if TYPE_CHECKING:
    from cpg.specs import PropValueType

type PropFieldInput = PropFieldSpec | str


def _value_type_for_key(key: str, prop_specs: Mapping[str, PropSpec]) -> PropValueType:
    spec = prop_specs.get(key)
    if spec is None:
        msg = f"Prop key {key!r} is not defined in the catalog."
        raise ValueError(msg)
    return spec.type


def _resolve_prop_field(
    key: str,
    source: PropFieldInput,
    *,
    prop_specs: Mapping[str, PropSpec],
) -> PropFieldSpec:
    if isinstance(source, PropFieldSpec):
        if source.prop_key != key:
            msg = f"PropFieldSpec key mismatch: {source.prop_key!r} != {key!r}"
            raise ValueError(msg)
        value_type = _value_type_for_key(key, prop_specs)
        if source.value_type is None:
            return replace(source, value_type=value_type)
        if source.value_type != value_type:
            msg = (
                f"PropFieldSpec value_type mismatch for {key!r}: "
                f"{source.value_type!r} != {value_type!r}"
            )
            raise ValueError(msg)
        return source
    value_type = _value_type_for_key(key, prop_specs)
    return PropFieldSpec(
        prop_key=key,
        source_col=source,
        value_type=value_type,
    )


def _fields_from_source_map(
    *,
    source_map: Mapping[str, PropFieldInput],
    prop_specs: Mapping[str, PropSpec],
    source_columns: Sequence[str] | None,
) -> tuple[PropFieldSpec, ...]:
    fields: list[PropFieldSpec] = []
    for key, source in source_map.items():
        field = _resolve_prop_field(key, source, prop_specs=prop_specs)
        _validate_source_column(field, source_columns=source_columns)
        fields.append(field)
    return tuple(fields)


def prop_fields_from_catalog(
    *,
    source_map: Mapping[str, PropFieldInput],
    source_columns: Sequence[str] | None = None,
    prop_specs: Mapping[str, PropSpec] | None = None,
) -> tuple[PropFieldSpec, ...]:
    """Build PropFieldSpec entries validated against the property catalog.

    Parameters
    ----------
    source_map:
        Mapping of prop keys to PropFieldSpec or source column strings.
    source_columns:
        Optional sequence of available source columns for validation.
    prop_specs:
        Optional property catalog overrides.

    Returns
    -------
    tuple[PropFieldSpec, ...]
        Prop field specs validated against the catalog.
    """
    resolved_specs = prop_specs or PROP_SPECS
    return _fields_from_source_map(
        source_map=source_map,
        prop_specs=resolved_specs,
        source_columns=source_columns,
    )


def _validate_source_column(
    field: PropFieldSpec,
    *,
    source_columns: Sequence[str] | None,
) -> None:
    if source_columns is None or field.source_col is None:
        return
    validate_required_items(
        [field.source_col],
        source_columns,
        item_label=f"source columns for prop {field.prop_key!r}",
        error_type=ValueError,
    )


__all__ = ["PropFieldInput", "prop_fields_from_catalog"]
