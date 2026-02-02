"""Contract finalization tests for semantic outputs."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.catalog.dataset_specs import dataset_spec
from semantics.pipeline import _finalize_df_to_contract


def test_finalize_df_to_contract_adds_missing_columns() -> None:
    """Finalize should align columns to the dataset contract."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "file_id": ["file_a"],
            "path": ["src/main.py"],
            "def_id": ["def_1"],
        },
        name="partial_defs",
    )
    df = ctx.table("partial_defs")
    finalized = _finalize_df_to_contract(ctx, df=df, view_name="dim_exported_defs_v1")

    contract = dataset_spec("dim_exported_defs_v1").contract()
    expected_schema = contract.schema
    if hasattr(expected_schema, "names"):
        expected_names = tuple(expected_schema.names)
    else:
        expected_names = tuple(field.name for field in expected_schema)
    result_schema = finalized.schema()
    if hasattr(result_schema, "names"):
        names = tuple(result_schema.names)
    else:
        names = tuple(field.name for field in result_schema)

    assert names == expected_names
    batch = finalized.collect()[0]
    assert batch.column("qname").to_pylist() == [None]
    assert batch.column("symbol_roles").to_pylist() == [None]
    assert batch.column("qname_source").to_pylist() == [None]
