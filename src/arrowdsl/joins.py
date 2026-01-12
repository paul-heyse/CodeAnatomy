"""Join helpers for Acero plans."""

from __future__ import annotations

from typing import Literal

from arrowdsl.acero import acero
from arrowdsl.plan import Plan
from arrowdsl.runtime import Ordering
from arrowdsl.specs import JoinSpec

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
