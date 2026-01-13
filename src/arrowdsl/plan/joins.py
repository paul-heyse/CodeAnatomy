"""Join helpers for plan and kernel lanes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import overload

from arrowdsl.compute.kernels import apply_join
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.join_specs import JoinOutputSpec, join_spec
from arrowdsl.plan.ops import JoinSpec, JoinType
from arrowdsl.plan.plan import Plan

JoinInput = TableLike | Plan
JoinOutput = TableLike | Plan


@dataclass(frozen=True)
class JoinConfig:
    """Join configuration for kernel-lane helpers."""

    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    output_suffix_for_right: str = ""

    @classmethod
    def from_sequences(
        cls,
        *,
        left_keys: Sequence[str],
        right_keys: Sequence[str],
        left_output: Sequence[str],
        right_output: Sequence[str],
        output_suffix_for_right: str | None = None,
    ) -> JoinConfig:
        """Build a JoinConfig from sequence inputs.

        Returns
        -------
        JoinConfig
            Normalized join configuration.
        """
        return cls(
            left_keys=tuple(left_keys),
            right_keys=tuple(right_keys),
            left_output=tuple(left_output),
            right_output=tuple(right_output),
            output_suffix_for_right=output_suffix_for_right or "",
        )

    @classmethod
    def on_keys(
        cls,
        *,
        keys: Sequence[str],
        left_output: Sequence[str],
        right_output: Sequence[str],
        output_suffix_for_right: str | None = None,
    ) -> JoinConfig:
        """Build a JoinConfig using identical left/right keys.

        Returns
        -------
        JoinConfig
            Join configuration for symmetric key joins.
        """
        return cls.from_sequences(
            left_keys=keys,
            right_keys=keys,
            left_output=left_output,
            right_output=right_output,
            output_suffix_for_right=output_suffix_for_right,
        )

    def to_spec(self, *, join_type: JoinType = "left outer") -> JoinSpec:
        """Return a JoinSpec for the stored configuration.

        Returns
        -------
        JoinSpec
            Join spec with normalized outputs and suffixes.
        """
        return join_spec(
            join_type=join_type,
            left_keys=self.left_keys,
            right_keys=self.right_keys,
            output=JoinOutputSpec(
                left_output=self.left_output,
                right_output=self.right_output,
                output_suffix_for_right=self.output_suffix_for_right or "",
            ),
        )


@overload
def left_join(
    left: Plan,
    right: Plan,
    *,
    config: JoinConfig,
    use_threads: bool = True,
    ctx: ExecutionContext | None = None,
) -> Plan: ...


@overload
def left_join(
    left: Plan,
    right: TableLike,
    *,
    config: JoinConfig,
    use_threads: bool = True,
    ctx: ExecutionContext | None = None,
) -> Plan: ...


@overload
def left_join(
    left: TableLike,
    right: Plan,
    *,
    config: JoinConfig,
    use_threads: bool = True,
    ctx: ExecutionContext | None = None,
) -> Plan: ...


@overload
def left_join(
    left: TableLike,
    right: TableLike,
    *,
    config: JoinConfig,
    use_threads: bool = True,
    ctx: ExecutionContext | None = None,
) -> TableLike: ...


def left_join(
    left: JoinInput,
    right: JoinInput,
    *,
    config: JoinConfig,
    use_threads: bool = True,
    ctx: ExecutionContext | None = None,
) -> JoinOutput:
    """Perform a left outer join in the kernel lane.

    Returns
    -------
    TableLike
        Joined table.
    """
    spec = config.to_spec(join_type="left outer")
    if isinstance(left, Plan) or isinstance(right, Plan):
        left_plan = left if isinstance(left, Plan) else Plan.table_source(left)
        right_plan = right if isinstance(right, Plan) else Plan.table_source(right)
        return left_plan.join(right_plan, spec=spec, ctx=ctx)
    return apply_join(left, right, spec=spec, use_threads=use_threads)


def join_plan(
    left: JoinInput,
    right: JoinInput,
    *,
    spec: JoinSpec,
    use_threads: bool = True,
    ctx: ExecutionContext | None = None,
) -> JoinOutput:
    """Join tables or plans using a shared JoinSpec.

    Returns
    -------
    TableLike | Plan
        Joined output, plan-backed when inputs are plan-backed.
    """
    if isinstance(left, Plan) or isinstance(right, Plan):
        left_plan = left if isinstance(left, Plan) else Plan.table_source(left)
        right_plan = right if isinstance(right, Plan) else Plan.table_source(right)
        return left_plan.join(right_plan, spec=spec, ctx=ctx)
    return apply_join(left, right, spec=spec, use_threads=use_threads)


__all__ = ["JoinConfig", "join_plan", "left_join"]
