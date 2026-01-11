"""Join helpers for Acero plans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from arrowdsl.acero import acero
from arrowdsl.plan import Plan
from arrowdsl.runtime import Ordering

type JoinType = Literal[
    "inner",
    "left outer",
    "right outer",
    "full outer",
    "left semi",
    "right semi",
    "left anti",
    "right anti",
]


@dataclass(frozen=True)
class JoinSpec:
    """Join specification for hash joins.

    Parameters
    ----------
    join_type:
        Join type string.
    left_keys:
        Left-side join keys.
    right_keys:
        Right-side join keys.
    left_output:
        Output columns from the left side.
    right_output:
        Output columns from the right side.
    output_suffix_for_left:
        Suffix for left output column name collisions.
    output_suffix_for_right:
        Suffix for right output column name collisions.
    """

    join_type: JoinType
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""

    def __post_init__(self) -> None:
        """Validate that left and right key counts match.

        Raises
        ------
        ValueError
            Raised when key lengths differ.
        """
        if len(self.left_keys) != len(self.right_keys):
            msg = "left_keys and right_keys must have the same length."
            raise ValueError(msg)


def hash_join(*, left: Plan, right: Plan, spec: JoinSpec, label: str = "") -> Plan:
    """Perform a hash join in the plan lane.

    Parameters
    ----------
    left:
        Left-side plan.
    right:
        Right-side plan.
    spec:
        Join specification.
    label:
        Optional plan label.

    Returns
    -------
    Plan
        Joined plan (unordered output).

    Raises
    ------
    TypeError
        Raised when one or both plans are missing Acero declarations.
    """
    if left.decl is None or right.decl is None:
        msg = "hash_join requires Acero-backed Plans (missing declarations)."
        raise TypeError(msg)

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
        decl=decl,
        label=label or f"{left.label}_join_{right.label}",
        ordering=Ordering.unordered(),
    )
