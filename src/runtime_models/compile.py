"""Runtime validation models for DataFusion compile options."""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import Field, TypeAdapter, model_validator

from runtime_models.base import RuntimeBase


class DataFusionCompileOptionsRuntime(RuntimeBase):
    """Validated compile-time options for DataFusion execution."""

    cache: bool | None = None
    cache_max_columns: int | None = Field(default=64, ge=0)
    param_identifier_allowlist: tuple[str, ...] | None = None
    prepared_statements: bool = True
    prepared_param_types: Mapping[str, str] | None = None
    sql_policy_name: str | None = None
    enforce_sql_policy: bool = True
    enforce_preflight: bool = True
    dialect: str = "datafusion"
    enable_rewrites: bool = True
    capture_explain: bool = False
    explain_analyze: bool = False
    capture_plan_artifacts: bool = False
    substrait_validation: bool = False
    diagnostics_allow_sql: bool = False
    capture_semantic_diff: bool = False
    prefer_substrait: bool = False
    prefer_ast_execution: bool = True
    record_substrait_gaps: bool = False
    dynamic_projection: bool = True

    @model_validator(mode="after")
    def _validate_cache(self) -> DataFusionCompileOptionsRuntime:
        if self.cache is False and self.cache_max_columns is not None:
            msg = "cache_max_columns requires cache=True"
            raise ValueError(msg)
        if not self.prepared_statements and self.prepared_param_types is not None:
            msg = "prepared_param_types requires prepared_statements=True"
            raise ValueError(msg)
        return self


COMPILE_OPTIONS_ADAPTER = TypeAdapter(DataFusionCompileOptionsRuntime)

__all__ = ["COMPILE_OPTIONS_ADAPTER", "DataFusionCompileOptionsRuntime"]
