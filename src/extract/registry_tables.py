"""Spec tables for extract dataset registry rows."""

from __future__ import annotations

from collections.abc import Mapping

from arrowdsl.spec.tables.extract import (
    ExtractDatasetRowSpec,
    dataset_rows_from_table,
    extract_dataset_table_from_rows,
)
from extract.registry_template_specs import DATASET_TEMPLATE_SPECS
from extract.registry_templates import expand_dataset_templates

DATASET_ROW_RECORDS: tuple[Mapping[str, object], ...] = ()

_TEMPLATE_ROW_RECORDS = expand_dataset_templates(DATASET_TEMPLATE_SPECS)

EXTRACT_DATASET_TABLE = extract_dataset_table_from_rows(
    (*DATASET_ROW_RECORDS, *_TEMPLATE_ROW_RECORDS)
)

DATASET_ROW_SPECS: tuple[ExtractDatasetRowSpec, ...] = dataset_rows_from_table(
    EXTRACT_DATASET_TABLE
)


__all__ = ["DATASET_ROW_SPECS", "EXTRACT_DATASET_TABLE"]
