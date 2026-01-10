"""Join plan specifications and helpers."""

from __future__ import annotations

from dataclasses import dataclass

from .plan import Plan
from .runtime import Ordering

JoinType = str  # e.g. "inner", "left outer", "right outer", "full outer", "left semi", "left anti"


@dataclass(frozen=True)
class JoinSpec:
    """Specification for hash join inputs and outputs."""

    join_type: JoinType
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""

    def __post_init__(self) -> None:
        """Validate that left and right key lengths match."""
        if len(self.left_keys) != len(self.right_keys):
            raise ValueError("left_keys and right_keys must have the same length")


def hash_join(*, left: Plan, right: Plan, spec: JoinSpec, label: str = "") -> Plan:
    """Build a hash join declaration within an Acero plan.

    Returns
    -------
    Plan
        A plan representing the hash join.

    Raises
    ------
    TypeError
        Raised when either plan lacks an Acero declaration.
    """
    from pyarrow import acero

    if left.decl is None or right.decl is None:
        raise TypeError("hash_join requires Acero-backed Plans (left.decl and right.decl)")

    decl = acero.Declaration(
        "hashjoin",
        acero.HashJoinNodeOptions(
            join_type=spec.join_type,
            left_keys=list(spec.left_keys),
            right_keys=list(spec.right_keys),
            left_output=list(spec.left_output),
            right_output=list(spec.right_output),
            output_suffix_for_left=spec.output_suffix_for_left,
            output_suffix_for_right=spec.output_suffix_for_right,
        ),
        inputs=[left.decl, right.decl],
    )
    return Plan(
        decl=decl, label=label or f"{left.label}_join_{right.label}", ordering=Ordering.unordered()
    )
