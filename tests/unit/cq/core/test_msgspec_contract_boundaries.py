from __future__ import annotations

import msgspec
import pytest
from tools.cq.core.cache.contracts import QueryEntityScanCacheV1, SgRecordCacheV1
from tools.cq.core.cache.policy import CqCachePolicyV1
from tools.cq.core.runtime.execution_policy import ParallelismPolicy
from tools.cq.core.structs import CqCacheStruct, CqOutputStruct, CqSettingsStruct
from tools.cq.search.contracts import CrossLanguageDiagnostic, PythonSemanticOverview


def test_struct_bases_are_msgspec_contract_types() -> None:
    assert issubclass(CqSettingsStruct, msgspec.Struct)
    assert issubclass(CqOutputStruct, msgspec.Struct)
    assert issubclass(CqCacheStruct, msgspec.Struct)


def test_cache_contract_rejects_unknown_fields() -> None:
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"record": "def", "extra": 1}, type=SgRecordCacheV1)


def test_cache_policy_rejects_unknown_fields() -> None:
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert(
            {
                "enabled": True,
                "directory": ".cq_cache",
                "shards": 8,
                "timeout_seconds": 0.05,
                "ttl_seconds": 900,
                "unexpected": "value",
            },
            type=CqCachePolicyV1,
        )


def test_meta_constraints_reject_invalid_numeric_values() -> None:
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert(
            {"cpu_workers": 0, "io_workers": 8, "semantic_request_workers": 2},
            type=ParallelismPolicy,
        )

    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"record": "def", "start_line": -1}, type=SgRecordCacheV1)


def test_unset_fields_are_omitted_by_default() -> None:
    diag_payload = msgspec.to_builtins(CrossLanguageDiagnostic())
    overview_payload = msgspec.to_builtins(PythonSemanticOverview())
    assert "feature" not in diag_payload
    assert "primary_symbol" not in overview_payload


def test_explicit_none_is_distinct_from_unset() -> None:
    diag_payload = msgspec.to_builtins(CrossLanguageDiagnostic(feature=None))
    overview_payload = msgspec.to_builtins(PythonSemanticOverview(primary_symbol=None))
    assert diag_payload["feature"] is None
    assert overview_payload["primary_symbol"] is None


def test_query_cache_round_trip_uses_typed_contracts() -> None:
    payload = QueryEntityScanCacheV1(
        records=[
            SgRecordCacheV1(
                record="def",
                kind="function",
                file="src/mod.py",
                start_line=10,
                start_col=0,
                end_line=20,
                end_col=0,
                text="def build_graph(): ...",
                rule_id="py_def_function",
            )
        ]
    )
    encoded = msgspec.json.encode(payload)
    decoded = msgspec.json.decode(encoded, type=QueryEntityScanCacheV1)
    assert decoded.records[0].record == "def"
    assert decoded.records[0].file == "src/mod.py"


def test_type_info_exposes_contract_shape_and_constraints() -> None:
    cache_info = msgspec.inspect.type_info(QueryEntityScanCacheV1)
    assert isinstance(cache_info, msgspec.inspect.StructType)
    assert cache_info.forbid_unknown_fields is True
    assert cache_info.fields[0].name == "records"
    assert isinstance(cache_info.fields[0].type, msgspec.inspect.ListType)
    assert isinstance(cache_info.fields[0].type.item_type, msgspec.inspect.StructType)

    policy_info = msgspec.inspect.type_info(ParallelismPolicy)
    assert isinstance(policy_info, msgspec.inspect.StructType)
    cpu_field = next(field for field in policy_info.fields if field.name == "cpu_workers")
    assert isinstance(cpu_field.type, msgspec.inspect.IntType)
    assert cpu_field.type.ge == 1
