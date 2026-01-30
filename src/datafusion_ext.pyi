
from collections.abc import Mapping
from typing import Any

IS_STUB: bool


def plugin_library_path() -> str: ...


def plugin_manifest(path: str | None = ...) -> dict[str, object]: ...


def registry_snapshot(ctx: Any) -> Mapping[str, object]: ...


def udf_docs_snapshot(ctx: Any) -> Mapping[str, object]: ...


def load_df_plugin(path: str) -> Any: ...


def register_df_plugin_udfs(
    ctx: Any, plugin: Any, options_json: str | None = ...
) -> None: ...


def register_df_plugin_table_functions(ctx: Any, plugin: Any) -> None: ...


def register_df_plugin_table_providers(
    ctx: Any,
    plugin: Any,
    table_names: list[str] | None = ...,
    options_json: dict[str, str] | None = ...,
) -> None: ...


def create_df_plugin_table_provider(
    plugin: Any, provider_name: str, options_json: str | None = ...
) -> Any: ...


def register_df_plugin(
    ctx: Any,
    plugin: Any,
    table_names: list[str] | None = ...,
    options_json: dict[str, str] | None = ...,
) -> None: ...


def __getattr__(name: str) -> Any: ...
