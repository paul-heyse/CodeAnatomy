"""Central registry for rule definitions across domains."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from ibis_engine.param_tables import ParamTablePolicy, ParamTableSpec
from relspec.list_filter_gate import ListFilterGatePolicy
from relspec.pipeline_policy import KernelLanePolicy
from relspec.rules.diagnostics import RuleDiagnostic, rule_diagnostic_table
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
    """Aggregates rule definitions across adapters."""

    adapters: Sequence[RuleAdapter]
    param_table_specs: Sequence[ParamTableSpec] = ()
    param_table_policy: ParamTablePolicy | None = None
    list_filter_gate_policy: ListFilterGatePolicy | None = None
    kernel_lane_policy: KernelLanePolicy | None = None
    engine_session: EngineSession | None = None

    def rule_definitions(self) -> tuple[RuleDefinition, ...]:
        """Return all rule definitions across adapters.

        Returns
        -------
        tuple[RuleDefinition, ...]
            Consolidated rule definitions.
        """
        rules = self._collect_rule_definitions()
        validate_rule_definitions(rules)
        diagnostics = rule_sqlglot_diagnostics(
            rules,
            config=SqlGlotDiagnosticsConfig(
                param_table_specs=self.param_table_specs,
                param_table_policy=self.param_table_policy,
                list_filter_gate_policy=self.list_filter_gate_policy,
                kernel_lane_policy=self.kernel_lane_policy,
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
        """Return domains represented by adapters.

        Returns
        -------
        tuple[RuleDomain, ...]
            Domains present in the registry.
        """
        return tuple(adapter.domain for adapter in self.adapters)

    def templates(self) -> tuple[RuleTemplateSpec, ...]:
        """Return template specs across adapters.

        Returns
        -------
        tuple[RuleTemplateSpec, ...]
            Template specs aggregated from adapters.
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
        """Return template diagnostics across adapters.

        Returns
        -------
        tuple[RuleDiagnostic, ...]
            Template diagnostics for all adapters.
        """
        specs = self._collect_template_specs()
        diagnostics: list[RuleDiagnostic] = []
        for adapter in self.adapters:
            diagnostics.extend(adapter.template_diagnostics())
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
        return rule_sqlglot_diagnostics(
            rules,
            config=SqlGlotDiagnosticsConfig(
                param_table_specs=self.param_table_specs,
                param_table_policy=self.param_table_policy,
                list_filter_gate_policy=self.list_filter_gate_policy,
                kernel_lane_policy=self.kernel_lane_policy,
                engine_session=self.engine_session,
            ),
        )

    def rule_diagnostics_table(self) -> pa.Table:
        """Return a diagnostics table for rule validation.

        Returns
        -------
        pyarrow.Table
            Diagnostics table for rule validation.
        """
        return rule_diagnostic_table(self.rule_diagnostics())

    def _collect_rule_definitions(self) -> tuple[RuleDefinition, ...]:
        """Collect rule definitions from all adapters.

        Returns
        -------
        tuple[RuleDefinition, ...]
            Consolidated rule definitions.
        """
        collected: list[RuleDefinition] = []
        for adapter in self.adapters:
            collected.extend(adapter.rule_definitions())
        return tuple(collected)

    def _collect_template_specs(self) -> tuple[RuleTemplateSpec, ...]:
        """Collect template specs from all adapters.

        Returns
        -------
        tuple[RuleTemplateSpec, ...]
            Consolidated template specs.
        """
        collected: list[RuleTemplateSpec] = []
        for adapter in self.adapters:
            collected.extend(adapter.templates())
        return tuple(collected)


def collect_rule_definitions(adapters: Iterable[RuleAdapter]) -> tuple[RuleDefinition, ...]:
    """Collect rule definitions from a sequence of adapters.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Consolidated rule definitions.
    """
    return RuleRegistry(adapters=tuple(adapters)).rule_definitions()


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
]
