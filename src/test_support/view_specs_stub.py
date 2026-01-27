"""Install a minimal ``schema_spec.view_specs`` stub for tests."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame
    from datafusion_engine.runtime import SessionRuntime


@dataclass(frozen=True)
class ViewSpecInputs:
    """Inputs used to construct a view specification."""

    ctx: SessionContext
    name: str
    builder: Callable[[SessionContext], DataFrame]
    schema: pa.Schema | None = None


@dataclass(frozen=True)
class ViewSpec:
    """Minimal view specification used in schema-registry tests."""

    name: str
    schema: pa.Schema | None = None
    builder: Callable[[SessionContext], DataFrame] | None = None

    def register(
        self,
        session_runtime: SessionRuntime,
        *,
        validate: bool = True,
        sql_options: object | None = None,
    ) -> None:
        """Register the view using the DataFusion IO adapter.

        Raises
        ------
        ValueError
            Raised when the view specification lacks a builder.
        """
        _ = validate, sql_options
        if self.builder is None:
            msg = f"View {self.name!r} missing builder."
            raise ValueError(msg)
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=session_runtime.ctx, profile=None)
        adapter.register_view(
            self.name,
            self.builder(session_runtime.ctx),
            overwrite=True,
            temporary=True,
        )


def view_spec_from_builder(inputs: ViewSpecInputs) -> ViewSpec:
    """Return a minimal view spec without importing heavy runtime modules.

    Returns
    -------
    ViewSpec
        Minimal view specification that defers registration to the IO adapter.
    """
    return ViewSpec(
        name=inputs.name,
        schema=inputs.schema,
        builder=inputs.builder,
    )


def _install_stub() -> None:
    if "schema_spec.view_specs" in sys.modules:
        return
    stub = ModuleType("schema_spec.view_specs")
    stub.__dict__["ViewSpecInputs"] = ViewSpecInputs
    stub.__dict__["ViewSpec"] = ViewSpec
    stub.__dict__["view_spec_from_builder"] = view_spec_from_builder
    sys.modules[stub.__name__] = stub


_install_stub()

__all__ = ["_install_stub"]
