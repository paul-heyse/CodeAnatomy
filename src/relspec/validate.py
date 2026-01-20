"""Validation gates for relspec registries."""

from __future__ import annotations

from dataclasses import dataclass

from relspec.registry.rules import RuleRegistry
from relspec.rules.coverage import RuleCoverageAssessment, assess_rule_coverage
from relspec.rules.diagnostics import RuleDiagnostic


@dataclass(frozen=True)
class ValidationReport:
    """Validation report for relspec registries."""

    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    coverage: RuleCoverageAssessment | None = None

    def raise_for_errors(self) -> None:
        """Raise when validation errors are present.

        Raises
        ------
        ValueError
            Raised when validation errors are present.
        """
        if not self.errors:
            return
        summary = "; ".join(self.errors)
        msg = f"Relspec validation failed: {summary}"
        raise ValueError(msg)


def validate_registry(
    registry: RuleRegistry,
    *,
    include_coverage: bool = True,
) -> ValidationReport:
    """Validate a rule registry and return a report.

    Parameters
    ----------
    registry
        Rule registry to validate.
    include_coverage
        Whether to include a rule coverage assessment.

    Returns
    -------
    ValidationReport
        Validation report for the registry.
    """
    errors: list[str] = []
    warnings: list[str] = []
    rule_diagnostics: tuple[RuleDiagnostic, ...] = ()
    template_diagnostics: tuple[RuleDiagnostic, ...] = ()
    try:
        rule_diagnostics = registry.rule_diagnostics()
    except ValueError as exc:
        errors.append(str(exc))
    try:
        template_diagnostics = registry.template_diagnostics()
    except ValueError as exc:
        errors.append(str(exc))
    for diag in (*rule_diagnostics, *template_diagnostics):
        if diag.severity == "error":
            errors.append(_format_diag(diag))
        else:
            warnings.append(_format_diag(diag))
    coverage: RuleCoverageAssessment | None = None
    if include_coverage:
        try:
            rules = registry.rule_definitions()
        except ValueError as exc:
            errors.append(str(exc))
        else:
            coverage = assess_rule_coverage(rules)
    return ValidationReport(errors=tuple(errors), warnings=tuple(warnings), coverage=coverage)


def _format_diag(diag: RuleDiagnostic) -> str:
    label = diag.rule_name or diag.template or "unknown"
    return f"{diag.domain}:{label}:{diag.message}"


__all__ = ["ValidationReport", "validate_registry"]
