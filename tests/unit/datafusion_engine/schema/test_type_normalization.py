"""Unit tests for contract-type normalization helpers."""

from __future__ import annotations

from datafusion_engine.schema.type_normalization import normalize_contract_type


def test_normalize_contract_type_list_variants() -> None:
    """Normalize list contract variants to canonical list syntax."""
    assert normalize_contract_type("list(item:utf8)") == "list<utf8>"
    assert normalize_contract_type("list<item:large_utf8>") == "list<large_utf8>"


def test_normalize_contract_type_map_and_struct_variants() -> None:
    """Normalize map and struct contract variants to canonical tokens."""
    assert normalize_contract_type("map<key:utf8,value:int64>") == "map"
    assert normalize_contract_type("struct<field:a:int64>") == "struct"
