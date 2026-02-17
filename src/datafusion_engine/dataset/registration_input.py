"""Input-plugin helpers for dataset registration."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.dataset.registry import DatasetCatalog

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

_INPUT_PLUGIN_PREFIXES = ("artifact://", "dataset://", "repo://")

try:
    from datafusion.input.base import BaseInputSource as _BaseInputSource
except ImportError:  # pragma: no cover - optional dependency
    _BaseInputSource = None
    _INPUT_PLUGIN_AVAILABLE = False
else:
    _INPUT_PLUGIN_AVAILABLE = True


class _DatasetInputSource:
    """Resolve dataset handles into registered tables."""

    def __init__(
        self,
        ctx: SessionContext,
        *,
        catalog: DatasetCatalog,
        runtime_profile: DataFusionRuntimeProfile | None,
    ) -> None:
        self._ctx = ctx
        self._catalog = catalog
        self._runtime_profile = runtime_profile

    def is_correct_input(
        self,
        input_item: object,
        table_name: str,
        **kwargs: object,
    ) -> bool:
        """Return True when the input matches a dataset registry handle."""
        _ = table_name, kwargs
        name = _dataset_name_from_input(input_item)
        return name is not None and self._catalog.has(name)

    def build_table(
        self,
        input_item: object,
        table_name: str,
        **kwargs: object,
    ) -> DataFrame:
        """Build a DataFusion DataFrame for a dataset registry handle.

        Returns:
            DataFusion table reference for ``table_name``.

        Raises:
            ValueError: If ``input_item`` is not a supported dataset handle.
        """
        _ = kwargs
        name = _dataset_name_from_input(input_item)
        if name is None:
            msg = f"Unsupported dataset handle: {input_item!r}."
            raise ValueError(msg)
        location = self._catalog.get(name)
        from datafusion_engine.dataset.registration_core import register_dataset_df

        return register_dataset_df(
            self._ctx,
            name=table_name,
            location=location,
            runtime_profile=self._runtime_profile,
        )


def _dataset_name_from_input(value: object) -> str | None:
    handle = str(value)
    for prefix in _INPUT_PLUGIN_PREFIXES:
        if handle.startswith(prefix):
            name = handle.removeprefix(prefix)
            return name if name else None
    return None


def dataset_input_plugin(
    catalog: DatasetCatalog,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> Callable[[SessionContext], None]:
    """Return a SessionContext installer for dataset input sources."""

    def _install(ctx: SessionContext) -> None:
        if not _INPUT_PLUGIN_AVAILABLE:
            return
        register = getattr(ctx, "register_input_source", None)
        if not callable(register):
            return
        register(
            _DatasetInputSource(
                ctx,
                catalog=catalog,
                runtime_profile=runtime_profile,
            )
        )

    return _install


def input_plugin_prefixes() -> tuple[str, ...]:
    """Return the registered input plugin prefixes."""
    return _INPUT_PLUGIN_PREFIXES


__all__ = [
    "dataset_input_plugin",
    "input_plugin_prefixes",
]
