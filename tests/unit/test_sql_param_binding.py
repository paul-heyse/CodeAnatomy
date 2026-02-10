"""Unit tests for DataFusion SQL parameter bindings."""

from __future__ import annotations

import pytest

from datafusion_engine.session.facade import DataFusionExecutionFacade
from datafusion_engine.tables.param import resolve_param_bindings
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion

datafusion = require_datafusion()


def test_param_allowlist_blocks_unknown_names() -> None:
    """Reject parameter bindings that are not allowlisted."""
    with pytest.raises(ValueError, match="allowlisted"):
        resolve_param_bindings({"val": 1}, allowlist=("other",))


def test_param_allowlist_allows_named_params() -> None:
    """Execute SQL when parameters are allowlisted."""
    bindings = resolve_param_bindings({"val": 1}, allowlist=("val",))
    assert bindings.param_values["val"] == 1


def test_ast_execution_lane() -> None:
    """Execute SQL via the AST execution lane."""
    profile = df_profile()
    ctx = profile.session_context()
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    bundle = facade.compile_to_bundle(lambda session: session.sql("SELECT 1 AS val"))
    result = facade.execute_plan_artifact(bundle)
    assert result.dataframe is not None
    table = result.dataframe.to_arrow_table()
    assert table.num_rows == 1
    assert table.column(0)[0].as_py() == 1
