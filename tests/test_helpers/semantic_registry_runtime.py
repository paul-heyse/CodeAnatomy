"""Helpers for registering semantic registry tables in DataFusion tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.arrow.interop import empty_table_for_schema
from datafusion_engine.io.adapter import DataFusionIOAdapter
from semantics.catalog.dataset_specs import dataset_specs
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import SessionRuntime


def _register_semantic_registry(ctx: SessionContext) -> None:
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    for spec in dataset_specs():
        if ctx.table_exist(spec.name):
            continue
        schema = spec.schema()
        table = empty_table_for_schema(schema)
        adapter.register_arrow_table(spec.name, table)


def semantic_registry_runtime() -> tuple[SessionContext, SessionRuntime]:
    """Return a DataFusion runtime with semantic registry tables registered.

    Returns
    -------
    tuple[SessionContext, SessionRuntime]
        DataFusion session context and runtime with registry tables registered.
    """
    require_datafusion()
    profile = df_profile()
    ctx = profile.session_context()
    _register_semantic_registry(ctx)
    return ctx, profile.session_runtime()


__all__ = ["semantic_registry_runtime"]
