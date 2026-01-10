from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, TYPE_CHECKING

from .plan import Plan
from .runtime import Ordering

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow.acero as acero


JoinType = str  # e.g. "inner", "left outer", "right outer", "full outer", "left semi", "left anti"


@dataclass(frozen=True)
class JoinSpec:
    join_type: JoinType
    left_keys: Tuple[str, ...]
    right_keys: Tuple[str, ...]
    left_output: Tuple[str, ...]
    right_output: Tuple[str, ...]
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""

    def __post_init__(self) -> None:
        if len(self.left_keys) != len(self.right_keys):
            raise ValueError("left_keys and right_keys must have the same length")


def hash_join(*, left: Plan, right: Plan, spec: JoinSpec, label: str = "") -> Plan:
    """Hash join inside Acero (plan lane). Output ordering is not predictable."""
    import pyarrow.acero as acero

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
    return Plan(decl=decl, label=label or f"{left.label}_join_{right.label}", ordering=Ordering.unordered())
