"""Relspec runtime composition helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ibis_engine.param_tables import ParamTableSpec
from relspec.config import RelspecConfig
from relspec.policies import PolicyRegistry
from relspec.registry.rules import RuleRegistry, default_rule_registry
from relspec.rules.compiler import RuleCompiler
from relspec.rules.handlers import default_rule_handlers
from relspec.schema_context import RelspecSchemaContext

if TYPE_CHECKING:
    from engine.session import EngineSession


@dataclass(frozen=True)
class RelspecRuntime:
    """Runtime bundle for relspec registry and compilation."""

    config: RelspecConfig
    registry: RuleRegistry
    compiler: RuleCompiler
    policies: PolicyRegistry


@dataclass(frozen=True)
class RelspecRuntimeOptions:
    """Optional inputs for composing a relspec runtime."""

    policies: PolicyRegistry | None = None
    param_table_specs: Sequence[ParamTableSpec] = ()
    engine_session: EngineSession | None = None
    schema_context: RelspecSchemaContext | None = None
    include_contract_rules: bool = True


def compose_relspec(
    *,
    config: RelspecConfig | None = None,
    options: RelspecRuntimeOptions | None = None,
) -> RelspecRuntime:
    """Compose a relspec runtime bundle.

    Returns
    -------
    RelspecRuntime
        Runtime bundle with registry and compiler wiring.
    """
    resolved_config = config or RelspecConfig()
    resolved_options = options or RelspecRuntimeOptions()
    resolved_policies = resolved_options.policies or PolicyRegistry()
    registry = default_rule_registry(
        param_table_specs=resolved_options.param_table_specs,
        config=resolved_config,
        engine_session=resolved_options.engine_session,
        schema_context=resolved_options.schema_context,
        include_contract_rules=resolved_options.include_contract_rules,
    )
    compiler = RuleCompiler(handlers=default_rule_handlers(policies=resolved_policies))
    return RelspecRuntime(
        config=resolved_config,
        registry=registry,
        compiler=compiler,
        policies=resolved_policies,
    )


__all__ = ["RelspecRuntime", "RelspecRuntimeOptions", "compose_relspec"]
