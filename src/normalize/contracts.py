"""Canonical normalized evidence schema and helpers."""

from __future__ import annotations

from functools import cache

from arrowdsl.core.interop import SchemaLike
from normalize.registry_runtime import dataset_contract, dataset_schema, dataset_spec
from schema_spec.system import Contract, DatasetSpec

NORMALIZE_EVIDENCE_NAME = "normalize_evidence_v1"


@cache
def normalize_evidence_spec() -> DatasetSpec:
    """Return the DatasetSpec for canonical normalized evidence.

    Returns
    -------
    DatasetSpec
        Dataset specification for canonical normalized evidence.
    """
    return dataset_spec(NORMALIZE_EVIDENCE_NAME)


def normalize_evidence_schema() -> SchemaLike:
    """Return the Arrow schema for canonical normalized evidence.

    Returns
    -------
    SchemaLike
        Arrow schema for canonical normalized evidence.
    """
    return dataset_schema(NORMALIZE_EVIDENCE_NAME)


def normalize_evidence_contract() -> Contract:
    """Return the Contract for canonical normalized evidence.

    Returns
    -------
    Contract
        Runtime contract for canonical normalized evidence.
    """
    return dataset_contract(NORMALIZE_EVIDENCE_NAME).to_contract()


__all__ = [
    "NORMALIZE_EVIDENCE_NAME",
    "normalize_evidence_contract",
    "normalize_evidence_schema",
    "normalize_evidence_spec",
]
