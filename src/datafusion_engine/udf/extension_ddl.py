"""Extension CREATE FUNCTION DDL helpers extracted from extension_core."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.types as patypes
from datafusion import SessionContext

from datafusion_engine.dataset.ddl_types import DDL_COMPLEX_TYPE_TOKENS, ddl_type_alias

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


def _ddl_type_name_from_arrow(dtype: pa.DataType) -> str | None:
    if patypes.is_dictionary(dtype):
        return _ddl_type_name(dtype.value_type)
    result: str | None = None
    if patypes.is_decimal(dtype):
        result = f"DECIMAL({dtype.precision},{dtype.scale})"
    elif patypes.is_timestamp(dtype):
        result = "TIMESTAMP"
    elif patypes.is_date(dtype):
        result = "DATE"
    elif patypes.is_time(dtype):
        result = "TIME"
    varchar_checks = (
        patypes.is_struct,
        patypes.is_list,
        patypes.is_large_list,
        patypes.is_fixed_size_list,
        patypes.is_map,
        patypes.is_union,
        patypes.is_binary,
        patypes.is_large_binary,
        patypes.is_fixed_size_binary,
    )
    if result is None and any(check(dtype) for check in varchar_checks):
        result = "VARCHAR"
    return result


def _ddl_type_name_from_string(dtype_name: str) -> str:
    if dtype_name.startswith("timestamp"):
        return "TIMESTAMP"
    if dtype_name.startswith("date"):
        return "DATE"
    if dtype_name.startswith("time"):
        return "TIME"
    if any(token in dtype_name for token in DDL_COMPLEX_TYPE_TOKENS) or dtype_name in {
        "any",
        "variant",
        "json",
    }:
        return "VARCHAR"
    alias = ddl_type_alias(dtype_name)
    return alias or dtype_name.upper()


def _ddl_type_name(dtype: object) -> str:
    if isinstance(dtype, pa.DataType):
        arrow_name = _ddl_type_name_from_arrow(dtype)
        if arrow_name is not None:
            return arrow_name
    dtype_name = str(dtype).strip().lower()
    return _ddl_type_name_from_string(dtype_name)


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
        FunctionArgSpec(name=name, dtype=_ddl_type_name(dtype))
        for name, dtype in zip(arg_names, spec.input_types, strict=False)
    )


def _ddl_return_type(spec: DataFusionUdfSpec) -> str | None:
    if spec.kind == "table":
        return None
    return _ddl_type_name(spec.return_type)


def _ddl_body_sql(target_name: str, arg_count: int, *, kind: str) -> str:
    if kind in {"window", "table"}:
        return f"'{target_name}'"
    if arg_count == 0:
        return f"{target_name}()"
    placeholders = ", ".join(f"${index}" for index in range(1, arg_count + 1))
    return f"{target_name}({placeholders})"


__all__: list[str] = []
