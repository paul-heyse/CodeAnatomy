"""Runtime-loaded DataFusion plugin helpers."""

from __future__ import annotations

import importlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, cast

from datafusion import SessionContext

logger = logging.getLogger(__name__)


class _PluginExtension(Protocol):
    def load_df_plugin(self, path: str) -> object: ...

    def register_df_plugin_udfs(self, ctx: SessionContext, handle: object) -> None: ...

    def register_df_plugin_table_functions(self, ctx: SessionContext, handle: object) -> None: ...

    def register_df_plugin_table_providers(
        self,
        ctx: SessionContext,
        handle: object,
        table_names: list[str] | None,
        options_json: dict[str, str] | None,
    ) -> None: ...


@dataclass(frozen=True)
class DataFusionPluginTableSpec:
    """Configuration for a table provider exported by a plugin."""

    name: str
    options: Mapping[str, str] = field(default_factory=dict)

    def options_json(self) -> str | None:
        """Return JSON options for the provider.

        Returns
        -------
        str | None
            JSON-encoded options, or None when unset.
        """
        if not self.options:
            return None
        return json.dumps(self.options, sort_keys=True)


@dataclass(frozen=True)
class DataFusionPluginSpec:
    """Configuration for a DataFusion plugin library."""

    path: str
    table_providers: tuple[DataFusionPluginTableSpec, ...] = ()
    enable_udfs: bool = True
    enable_table_functions: bool = True
    enable_table_providers: bool = True

    def table_provider_names(self) -> tuple[str, ...]:
        """Return the explicit table provider names to register.

        Returns
        -------
        tuple[str, ...]
            Explicit table provider names.
        """
        return tuple(spec.name for spec in self.table_providers)

    def table_provider_options(self) -> dict[str, str]:
        """Return JSON options for the configured table providers.

        Returns
        -------
        dict[str, str]
            Mapping of table provider names to JSON-encoded options.
        """
        options: dict[str, str] = {}
        for spec in self.table_providers:
            payload = spec.options_json()
            if payload is not None:
                options[spec.name] = payload
        return options


class DataFusionPluginManager:
    """Load and register DataFusion plugin libraries."""

    def __init__(self, specs: Sequence[DataFusionPluginSpec]) -> None:
        self.specs = tuple(specs)
        self._handles: dict[str, object] = {}

    def load_all(self) -> None:
        """Load all plugin libraries and cache the handles."""
        if not self.specs:
            return
        module = _load_extension()
        for spec in self.specs:
            if spec.path in self._handles:
                continue
            logger.info("Loading DataFusion plugin: %s", spec.path)
            self._handles[spec.path] = module.load_df_plugin(spec.path)

    def register_all(self, ctx: SessionContext) -> None:
        """Register all configured plugins against a SessionContext."""
        if not self.specs:
            return
        module = _load_extension()
        self.load_all()
        for spec in self.specs:
            handle = self._handles[spec.path]
            if spec.enable_udfs:
                module.register_df_plugin_udfs(ctx, handle)
            if spec.enable_table_functions:
                module.register_df_plugin_table_functions(ctx, handle)
            if spec.enable_table_providers:
                table_names = spec.table_provider_names()
                options = spec.table_provider_options()
                module.register_df_plugin_table_providers(
                    ctx,
                    handle,
                    list(table_names) if table_names else None,
                    options or None,
                )


def _load_extension() -> _PluginExtension:
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:  # pragma: no cover - validated at call sites
        msg = "DataFusion plugin manager requires datafusion_ext."
        raise RuntimeError(msg) from exc
    return cast("_PluginExtension", module)


__all__ = [
    "DataFusionPluginManager",
    "DataFusionPluginSpec",
    "DataFusionPluginTableSpec",
]
