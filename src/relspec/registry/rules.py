"""Central registry for rule definitions across domains."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from ibis_engine.param_tables import ParamTableSpec
from relspec.config import RelspecConfig
from relspec.rules.bundles import RuleBundle
from relspec.rules.contract_rules import rules_from_contracts
from relspec.rules.diagnostics import RuleDiagnostic, rule_diagnostic_table
from relspec.rules.discovery import discover_bundles
from relspec.rules.spec_tables import rule_definition_table
from relspec.rules.templates import (
    RuleTemplateSpec,
    rule_template_table,
    validate_template_specs,
)
from relspec.rules.validation import (
    SqlGlotDiagnosticsConfig,
    rule_sqlglot_diagnostics,
    validate_rule_definitions,
)
from relspec.schema_context import RelspecSchemaContext

if TYPE_CHECKING:
    from engine.session import EngineSession
    from relspec.rules.definitions import RuleDefinition, RuleDomain


class RuleAdapter(Protocol):
    """Adapter interface for domain-specific rule definitions."""

    @property
    def domain(self) -> RuleDomain:
        """Return the domain name for the adapter."""
        ...

    def rule_definitions(self) -> Sequence[RuleDefinition]:
        """Return rule definitions for the adapter."""
        ...

    def templates(self) -> Sequence[RuleTemplateSpec]:
        """Return any template specs for the adapter."""
        ...

    def template_diagnostics(self) -> Sequence[RuleDiagnostic]:
        """Return template diagnostics for the adapter."""
        ...


@dataclass(frozen=True)
class RuleRegistry:
    """Aggregates rule definitions across bundles."""

    bundles: Sequence[RuleBundle]
    param_table_specs: Sequence[ParamTableSpec] = ()
    config: RelspecConfig | None = None
    engine_session: EngineSession | None = None
    schema_context: RelspecSchemaContext | None = None
    include_contract_rules: bool = True

    def rule_definitions(self) -> tuple[RuleDefinition, ...]:
        """Return all rule definitions across bundles.

        Returns
        -------
        tuple[RuleDefinition, ...]
            Consolidated rule definitions.
        """
        rules = self._collect_rule_definitions()
        validate_rule_definitions(rules)
        config = self._resolved_config()
        diagnostics = rule_sqlglot_diagnostics(
            rules,
            config=SqlGlotDiagnosticsConfig(
                param_table_specs=self.param_table_specs,
                param_table_policy=config.param_table_policy,
                list_filter_gate_policy=config.list_filter_gate_policy,
                kernel_lane_policy=config.kernel_lane_policy,
                engine_session=self.engine_session,
            ),
        )
        _raise_rule_errors(diagnostics)
        return rules

    def rules_for_domain(self, domain: RuleDomain) -> tuple[RuleDefinition, ...]:
        """Return rule definitions for a single domain.

        Returns
        -------
        tuple[RuleDefinition, ...]
            Rule definitions for the domain.
        """
        return tuple(rule for rule in self.rule_definitions() if rule.domain == domain)

    def rule_table(self) -> pa.Table:
        """Return the canonical rule definition table.

        Returns
        -------
        pyarrow.Table
            Table containing all rule definitions.
        """
        return rule_definition_table(self.rule_definitions())

    def domains(self) -> tuple[RuleDomain, ...]:
        """Return domains represented by bundles.

        Returns
        -------
        tuple[RuleDomain, ...]
            Domains present in the registry.
        """
        return tuple(bundle.domain for bundle in self.bundles)

    def templates(self) -> tuple[RuleTemplateSpec, ...]:
        """Return template specs across bundles.

        Returns
        -------
        tuple[RuleTemplateSpec, ...]
            Template specs aggregated from bundles.
        """
        specs = self._collect_template_specs()
        diagnostics = self.template_diagnostics()
        _raise_template_errors(diagnostics)
        return specs

    def template_table(self) -> pa.Table:
        """Return the centralized template catalog table.

        Returns
        -------
        pyarrow.Table
            Template catalog table.
        """
        return rule_template_table(self.templates())

    def template_diagnostics(self) -> tuple[RuleDiagnostic, ...]:
        """Return template diagnostics across bundles.

        Returns
        -------
        tuple[RuleDiagnostic, ...]
            Template diagnostics for all bundles.
        """
        specs = self._collect_template_specs()
        diagnostics: list[RuleDiagnostic] = []
        for bundle in self.bundles:
            diagnostics.extend(bundle.build_diagnostics())
        diagnostics.extend(validate_template_specs(specs))
        return tuple(diagnostics)

    def template_diagnostics_table(self) -> pa.Table:
        """Return a diagnostics table for template issues.

        Returns
        -------
        pyarrow.Table
            Diagnostics table for template issues.
        """
        return rule_diagnostic_table(self.template_diagnostics())

    def rule_diagnostics(self) -> tuple[RuleDiagnostic, ...]:
        """Return SQLGlot diagnostics for rule definitions.

        Returns
        -------
        tuple[RuleDiagnostic, ...]
            Diagnostics emitted for rule validation.
        """
        rules = self._collect_rule_definitions()
        validate_rule_definitions(rules)
        config = self._resolved_config()
        return rule_sqlglot_diagnostics(
            rules,
            config=SqlGlotDiagnosticsConfig(
                param_table_specs=self.param_table_specs,
                param_table_policy=config.param_table_policy,
                list_filter_gate_policy=config.list_filter_gate_policy,
                kernel_lane_policy=config.kernel_lane_policy,
                engine_session=self.engine_session,
            ),
        )

    def _resolved_config(self) -> RelspecConfig:
        return self.config or RelspecConfig()

    def _resolved_schema_context(self) -> RelspecSchemaContext:
        from datafusion_engine.runtime import DataFusionRuntimeProfile

        if self.schema_context is not None:
            return self.schema_context
        if self.engine_session is not None:
            return RelspecSchemaContext.from_engine_session(self.engine_session)
        profile = DataFusionRuntimeProfile()
        return RelspecSchemaContext.from_session(profile.session_context())

    def rule_diagnostics_table(self) -> pa.Table:
        """Return a diagnostics table for rule validation.

        Returns
        -------
        pyarrow.Table
            Diagnostics table for rule validation.
        """
        return rule_diagnostic_table(self.rule_diagnostics())

    def _collect_rule_definitions(self) -> tuple[RuleDefinition, ...]:
        """Collect rule definitions from all bundles.

        Returns
        -------
        tuple[RuleDefinition, ...]
            Consolidated rule definitions.
        """
        collected: list[RuleDefinition] = []
        for bundle in self.bundles:
            collected.extend(bundle.build_rules())
        if self.include_contract_rules:
            collected.extend(rules_from_contracts(self._resolved_schema_context()))
        return tuple(collected)

    def _collect_template_specs(self) -> tuple[RuleTemplateSpec, ...]:
        """Collect template specs from all bundles.

        Returns
        -------
        tuple[RuleTemplateSpec, ...]
            Consolidated template specs.
        """
        collected: list[RuleTemplateSpec] = []
        for bundle in self.bundles:
            collected.extend(bundle.build_templates())
        return tuple(collected)


def collect_rule_definitions(bundles: Iterable[RuleBundle]) -> tuple[RuleDefinition, ...]:
    """Collect rule definitions from a sequence of bundles.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Consolidated rule definitions.
    """
    return RuleRegistry(bundles=tuple(bundles)).rule_definitions()


def default_rule_registry(
    *,
    param_table_specs: Sequence[ParamTableSpec] = (),
    config: RelspecConfig | None = None,
    engine_session: EngineSession | None = None,
    schema_context: RelspecSchemaContext | None = None,
    include_contract_rules: bool = True,
) -> RuleRegistry:
    """Build the default rule registry using discovered bundles.

    Returns
    -------
    RuleRegistry
        Registry populated with discovered rule bundles.
    """
    bundles = discover_bundles()
    return RuleRegistry(
        bundles=bundles,
        param_table_specs=param_table_specs,
        config=config,
        engine_session=engine_session,
        schema_context=schema_context,
        include_contract_rules=include_contract_rules,
    )


def _raise_template_errors(diagnostics: Sequence[RuleDiagnostic]) -> None:
    """Raise on template diagnostics errors.

    Parameters
    ----------
    diagnostics
        Diagnostics to inspect.

    Raises
    ------
    ValueError
        Raised when template diagnostics contain errors.
    """
    errors = [diag for diag in diagnostics if diag.severity == "error"]
    if not errors:
        return
    summary = ", ".join(diag.message for diag in errors)
    msg = f"Template diagnostics reported errors: {summary}"
    raise ValueError(msg)


def _raise_rule_errors(diagnostics: Sequence[RuleDiagnostic]) -> None:
    """Raise on rule diagnostics errors.

    Parameters
    ----------
    diagnostics
        Diagnostics to inspect.

    Raises
    ------
    ValueError
        Raised when rule diagnostics contain errors.
    """
    errors = [diag for diag in diagnostics if diag.severity == "error"]
    if not errors:
        return
    summary = ", ".join(f"{diag.rule_name or 'unknown'}: {diag.message}" for diag in errors)
    msg = f"Rule diagnostics reported errors: {summary}"
    raise ValueError(msg)


__all__ = [
    "RuleAdapter",
    "RuleRegistry",
    "RuleTemplateSpec",
    "collect_rule_definitions",
    "default_rule_registry",
]
