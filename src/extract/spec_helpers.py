"""Helpers for registering extractor datasets."""

from __future__ import annotations

from arrowdsl.schema.metadata import (
    extractor_metadata_spec,
    infer_ordering_keys,
    merge_metadata_specs,
    options_hash,
    options_metadata_spec,
    ordering_metadata_spec,
)
from arrowdsl.spec.factories import DatasetRegistration, register_dataset

__all__ = [
    "DatasetRegistration",
    "extractor_metadata_spec",
    "infer_ordering_keys",
    "merge_metadata_specs",
    "options_hash",
    "options_metadata_spec",
    "ordering_metadata_spec",
    "register_dataset",
]
