"""Extension CREATE FUNCTION DDL helpers extracted from extension_core."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.dataset.ddl_types import ddl_type_name

if TYPE_CHECKING:
    from typing import Protocol

    from datafusion_engine.udf.factory import CreateFunctionConfig, FunctionArgSpec
    from datafusion_engine.udf.metadata import DataFusionUdfSpec

    class RegisterFunction(Protocol):
        def __call__(self, ctx: SessionContext, *, config: CreateFunctionConfig) -> None: ...


def _register_udf_specs(
    ctx: SessionContext,
    *,
    specs: Sequence[DataFusionUdfSpec],
    replace: bool,
    register_fn: RegisterFunction,
) -> None:
    for spec in specs:
        if spec.kind == "table":
            continue
        config = _ddl_config_for_spec(spec, target_name=spec.engine_name, replace=replace)
        register_fn(ctx, config=config)


def _register_udf_aliases(
    ctx: SessionContext,
    *,
    spec_map: Mapping[str, DataFusionUdfSpec],
    snapshot: Mapping[str, object],
    replace: bool,
    register_fn: RegisterFunction,
) -> None:
    alias_map = snapshot.get("aliases")
    if not isinstance(alias_map, Mapping):
        return
    for base_name, aliases in alias_map.items():
        if not isinstance(base_name, str):
            continue
        spec = spec_map.get(base_name)
        if spec is None or spec.kind == "table":
            continue
        for alias in _alias_list(aliases):
            if alias == base_name:
                continue
            config = _ddl_config_for_spec(
                spec,
                target_name=base_name,
                name_override=alias,
                replace=replace,
            )
            register_fn(ctx, config=config)


def _alias_list(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value if item is not None)
    return ()


def _ddl_config_for_spec(
    spec: DataFusionUdfSpec,
    *,
    target_name: str,
    name_override: str | None = None,
    replace: bool,
) -> CreateFunctionConfig:
    from datafusion_engine.udf.factory import CreateFunctionConfig

    args = _ddl_args(spec)
    return CreateFunctionConfig(
        name=name_override or spec.engine_name,
        args=args,
        return_type=_ddl_return_type(spec),
        returns_table=spec.kind == "table",
        body_sql=_ddl_body_sql(target_name, len(args), kind=spec.kind),
        language=None,
        volatility=spec.volatility,
        replace=replace,
    )


def _ddl_args(spec: DataFusionUdfSpec) -> tuple[FunctionArgSpec, ...]:
    from datafusion_engine.udf.factory import FunctionArgSpec

    arg_names = spec.arg_names
    if arg_names is None or len(arg_names) != len(spec.input_types):
        arg_names = tuple(f"arg{idx}" for idx in range(len(spec.input_types)))
    return tuple(
        FunctionArgSpec(name=name, dtype=ddl_type_name(dtype))
        for name, dtype in zip(arg_names, spec.input_types, strict=False)
    )


def _ddl_return_type(spec: DataFusionUdfSpec) -> str | None:
    if spec.kind == "table":
        return None
    return ddl_type_name(spec.return_type)


def _ddl_body_sql(target_name: str, arg_count: int, *, kind: str) -> str:
    if kind in {"window", "table"}:
        return f"'{target_name}'"
    if arg_count == 0:
        return f"{target_name}()"
    placeholders = ", ".join(f"${index}" for index in range(1, arg_count + 1))
    return f"{target_name}({placeholders})"


__all__: list[str] = []
