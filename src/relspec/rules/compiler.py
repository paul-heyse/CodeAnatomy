"""Central rule compilation interfaces."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from arrowdsl.core.context import ExecutionContext

if TYPE_CHECKING:
    from relspec.rules.definitions import RuleDefinition, RuleDomain


class RuleHandler(Protocol):
    """Protocol for domain-specific rule handlers."""

    @property
    def domain(self) -> RuleDomain:
        """Return the handler domain."""
        ...

    def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> object:
        """Compile a single rule definition."""
        ...


@dataclass(frozen=True)
class RuleCompiler:
    """Dispatch rule compilation to domain handlers."""

    handlers: Mapping[RuleDomain, RuleHandler]

    def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> object:
        """Compile a rule definition using the registered handler.

        Returns
        -------
        object
            Compiled rule artifact produced by the handler.

        Raises
        ------
        KeyError
            Raised when no handler is registered for the rule domain.
        """
        handler = self.handlers.get(rule.domain)
        if handler is None:
            msg = f"No rule handler registered for domain {rule.domain!r}."
            raise KeyError(msg)
        return handler.compile_rule(rule, ctx=ctx)

    def compile_rules(
        self, rules: Sequence[RuleDefinition], *, ctx: ExecutionContext
    ) -> tuple[object, ...]:
        """Compile multiple rule definitions.

        Returns
        -------
        tuple[object, ...]
            Compiled rule artifacts.
        """
        return tuple(self.compile_rule(rule, ctx=ctx) for rule in rules)


__all__ = ["RuleCompiler", "RuleHandler"]
