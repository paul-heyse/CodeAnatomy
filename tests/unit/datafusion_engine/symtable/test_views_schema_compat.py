"""Coverage for canonical symtable binding schema requirements."""

from __future__ import annotations

import pytest
from datafusion import SessionContext

from datafusion_engine.symtable.views import (
    symtable_binding_resolutions_df,
    symtable_def_sites_df,
    symtable_use_sites_df,
)


def _register_defs_and_refs(ctx: SessionContext) -> None:
    ctx.from_pydict(
        {
            "def_id": ["def_x"],
            "path": ["f.py"],
            "name": ["x"],
            "name_bstart": [10],
            "name_bend": [11],
            "def_bstart": [8],
            "def_bend": [20],
            "kind": ["function"],
        },
        name="cst_defs",
    )
    ctx.from_pydict(
        {
            "ref_id": ["ref_x"],
            "path": ["f.py"],
            "ref_text": ["x"],
            "bstart": [100],
            "bend": [101],
            "expr_ctx": ["Load"],
        },
        name="cst_refs",
    )


def _canonical_bindings_payload() -> dict[str, list[object]]:
    return {
        "file_id": ["file_1"],
        "path": ["f.py"],
        "scope_id": ["scope_1"],
        "binding_id": ["binding_x"],
        "name": ["x"],
        "binding_kind": ["local"],
        "declared_here": [True],
    }


def _legacy_bindings_payload() -> dict[str, list[object]]:
    return {
        "file_id": ["file_1"],
        "path": ["f.py"],
        "scope_id": ["scope_1"],
        "binding_id": ["binding_x"],
        "binding_name": ["x"],
        "binding_kind": ["local"],
        "binding_is_global": [False],
        "binding_is_import": [False],
        "binding_is_parameter": [False],
        "binding_is_nonlocal": [False],
        "binding_is_referenced": [True],
        "binding_is_annotated": [False],
        "binding_is_assigned": [True],
    }


@pytest.mark.parametrize(
    "bindings_payload",
    [_canonical_bindings_payload()],
    ids=["canonical"],
)
def test_symtable_sites_accept_canonical_binding_columns(
    bindings_payload: dict[str, list[object]],
) -> None:
    """Test symtable sites accept canonical binding columns."""
    ctx = SessionContext()
    ctx.from_pydict(bindings_payload, name="symtable_bindings")
    _register_defs_and_refs(ctx)

    def_sites = symtable_def_sites_df(ctx).to_arrow_table()
    use_sites = symtable_use_sites_df(ctx).to_arrow_table()

    assert def_sites.num_rows == 1
    assert use_sites.num_rows == 1
    assert {"name", "binding_id", "def_id"}.issubset(def_sites.column_names)
    assert {"name", "binding_id", "ref_id"}.issubset(use_sites.column_names)
    assert def_sites.to_pydict()["name"] == ["x"]
    assert use_sites.to_pydict()["name"] == ["x"]


def test_symtable_sites_reject_legacy_binding_columns() -> None:
    """Test symtable sites reject legacy binding columns."""
    ctx = SessionContext()
    ctx.from_pydict(_legacy_bindings_payload(), name="symtable_bindings")
    _register_defs_and_refs(ctx)

    with pytest.raises(ValueError, match="must expose canonical columns"):
        _ = symtable_def_sites_df(ctx).to_arrow_table()
    with pytest.raises(ValueError, match="must expose canonical columns"):
        _ = symtable_use_sites_df(ctx).to_arrow_table()


def test_symtable_binding_resolutions_require_canonical_binding_columns() -> None:
    """Test symtable binding resolutions require canonical binding columns."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "file_id": ["file_1", "file_1"],
            "path": ["f.py", "f.py"],
            "scope_id": ["fn_scope", "module_scope"],
            "binding_id": ["binding_global_ref", "binding_declared"],
            "binding_name": ["x", "x"],
            "binding_kind": ["global_ref", "local"],
            "binding_is_global": [True, False],
            "binding_is_import": [False, False],
            "binding_is_parameter": [False, False],
            "binding_is_nonlocal": [False, False],
            "binding_is_referenced": [True, True],
            "binding_is_annotated": [False, False],
            "binding_is_assigned": [False, True],
        },
        name="symtable_bindings",
    )
    ctx.from_pydict(
        {
            "path": ["f.py", "f.py"],
            "scope_id": ["module_scope", "fn_scope"],
            "scope_type": ["MODULE", "FUNCTION"],
        },
        name="symtable_scopes",
    )
    ctx.from_pydict(
        {
            "path": ["f.py"],
            "child_scope_id": ["fn_scope"],
            "parent_scope_id": ["module_scope"],
        },
        name="symtable_scope_edges",
    )

    with pytest.raises(ValueError, match="must expose canonical columns"):
        _ = symtable_binding_resolutions_df(ctx).to_arrow_table()
