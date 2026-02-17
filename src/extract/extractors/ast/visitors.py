"""AST walk result containers and visitor error types."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field


@dataclass(frozen=True)
class _AstWalkResult:
    nodes: list[dict[str, object]]
    edges: list[dict[str, object]]
    docstrings: list[dict[str, object]]
    imports: list[dict[str, object]]
    defs: list[dict[str, object]]
    calls: list[dict[str, object]]
    type_ignores: list[dict[str, object]]


@dataclass
class _AstWalkAccumulator:
    nodes: list[dict[str, object]] = dataclass_field(default_factory=list)
    edges: list[dict[str, object]] = dataclass_field(default_factory=list)
    docstrings: list[dict[str, object]] = dataclass_field(default_factory=list)
    imports: list[dict[str, object]] = dataclass_field(default_factory=list)
    defs: list[dict[str, object]] = dataclass_field(default_factory=list)
    calls: list[dict[str, object]] = dataclass_field(default_factory=list)
    type_ignores: list[dict[str, object]] = dataclass_field(default_factory=list)

    def to_result(self) -> _AstWalkResult:
        return _AstWalkResult(
            nodes=self.nodes,
            edges=self.edges,
            docstrings=self.docstrings,
            imports=self.imports,
            defs=self.defs,
            calls=self.calls,
            type_ignores=self.type_ignores,
        )


class AstLimitError(ValueError):
    """Raised when AST extraction exceeds configured limits."""


__all__ = ["AstLimitError"]
