"""Derived normalize dataset specs for DataFusion registration."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from schema_spec.specs import ArrowFieldSpec, call_span_bundle
from schema_spec.system import DatasetSpec, make_dataset_spec, make_table_spec

SCHEMA_VERSION: int = 1


@cache
def qname_dim_spec() -> DatasetSpec:
    """Return the dataset spec for qualified name dimensions.

    Returns
    -------
    DatasetSpec
        Dataset spec for the qualified name dimension table.
    """
    return make_dataset_spec(
        table_spec=make_table_spec(
            name="dim_qualified_names_v1",
            version=SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="qname_id", dtype=pa.string()),
                ArrowFieldSpec(name="qname", dtype=pa.string()),
            ],
        )
    )


@cache
def callsite_qname_candidates_spec() -> DatasetSpec:
    """Return the dataset spec for callsite qname candidates.

    Returns
    -------
    DatasetSpec
        Dataset spec for callsite qname candidates.
    """
    return make_dataset_spec(
        table_spec=make_table_spec(
            name="callsite_qname_candidates_v1",
            version=SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="call_id", dtype=pa.string()),
                ArrowFieldSpec(name="qname", dtype=pa.string()),
                ArrowFieldSpec(name="path", dtype=pa.string()),
                *call_span_bundle().fields,
                ArrowFieldSpec(name="qname_source", dtype=pa.string()),
            ],
        )
    )


def normalize_derived_specs() -> tuple[DatasetSpec, ...]:
    """Return derived normalize dataset specs.

    Returns
    -------
    tuple[DatasetSpec, ...]
        Derived dataset specs for normalize pipelines.
    """
    return (qname_dim_spec(), callsite_qname_candidates_spec())


__all__ = [
    "callsite_qname_candidates_spec",
    "normalize_derived_specs",
    "qname_dim_spec",
]
