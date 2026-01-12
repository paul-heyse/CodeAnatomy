"""Schema specification models and adapters."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from schema_spec.catalogs import ContractCatalogSpec
from schema_spec.contracts import ContractSpec, DedupeSpecSpec, SortKeySpec
from schema_spec.core import ArrowFieldSpec, TableSchemaSpec
from schema_spec.fields import DICT_STRING, PROVENANCE_COLS, FieldBundle
from schema_spec.registry import GLOBAL_SCHEMA_REGISTRY, SchemaRegistry

if TYPE_CHECKING:
    from schema_spec.factories import (
        TableSpecConstraints,
        VirtualFieldSpec,
        make_contract_spec,
        make_table_spec,
        query_spec_for_table,
        table_spec_from_schema,
    )

__all__ = [
    "DICT_STRING",
    "GLOBAL_SCHEMA_REGISTRY",
    "PROVENANCE_COLS",
    "ArrowFieldSpec",
    "ContractCatalogSpec",
    "ContractSpec",
    "DedupeSpecSpec",
    "FieldBundle",
    "SchemaRegistry",
    "SortKeySpec",
    "TableSchemaSpec",
    "TableSpecConstraints",
    "VirtualFieldSpec",
    "make_contract_spec",
    "make_table_spec",
    "query_spec_for_table",
    "table_spec_from_schema",
]


def __getattr__(name: str) -> object:
    if name in {
        "TableSpecConstraints",
        "VirtualFieldSpec",
        "make_contract_spec",
        "make_table_spec",
        "query_spec_for_table",
        "table_spec_from_schema",
    }:
        factories = importlib.import_module("schema_spec.factories")
        return getattr(factories, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
