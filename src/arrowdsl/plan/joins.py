"""Join helpers for plan and kernel lanes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import overload

from arrowdsl.compute.kernels import apply_join
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.ops import JoinSpec, JoinType
from arrowdsl.plan.plan import Plan

JoinInput = TableLike | Plan
JoinOutput = TableLike | Plan

CODE_UNIT_META_COLUMNS: tuple[str, ...] = ("code_unit_id", "file_id", "path")
PATH_META_COLUMNS: tuple[str, ...] = ("path", "file_id")


@dataclass(frozen=True)
class JoinOutputSpec:
    """Output column selections and suffixes for joins."""

    left_output: Sequence[str] = ()
    right_output: Sequence[str] = ()
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""


def join_spec(
    *,
    join_type: JoinType,
    left_keys: Sequence[str],
    right_keys: Sequence[str] | None = None,
    output: JoinOutputSpec | None = None,
) -> JoinSpec:
    """Return a JoinSpec from sequence inputs.

    Returns
    -------
    JoinSpec
        Normalized join spec.
    """
    right_keys_seq = right_keys if right_keys is not None else left_keys
    output = output or JoinOutputSpec()
    return JoinSpec(
        join_type=join_type,
        left_keys=tuple(left_keys),
        right_keys=tuple(right_keys_seq),
        left_output=tuple(output.left_output),
        right_output=tuple(output.right_output),
        output_suffix_for_left=output.output_suffix_for_left,
        output_suffix_for_right=output.output_suffix_for_right,
    )


def join_spec_for_keys(
    *,
    keys: Sequence[str],
    left_out: Sequence[str],
    right_out: Sequence[str],
    output_suffix_for_left: str = "",
    output_suffix_for_right: str = "",
) -> JoinSpec:
    """Return a left-outer JoinSpec with symmetric keys.

    Returns
    -------
    JoinSpec
        Join spec with left-outer semantics.
    """
    return join_spec(
        join_type="left outer",
        left_keys=keys,
        right_keys=keys,
        output=JoinOutputSpec(
            left_output=left_out,
            right_output=right_out,
            output_suffix_for_left=output_suffix_for_left,
            output_suffix_for_right=output_suffix_for_right,
        ),
    )


def code_unit_meta_config(
    left_columns: Sequence[str],
    right_columns: Sequence[str],
    *,
    key: str = "code_unit_id",
    output_suffix_for_right: str = "",
) -> JoinConfig | None:
    """Return a JoinConfig for code-unit metadata joins.

    Returns
    -------
    JoinConfig | None
        Join configuration or ``None`` when inputs are insufficient.
    """
    left_cols = tuple(left_columns)
    right_cols = tuple(right_columns)
    if key not in left_cols or key not in right_cols:
        return None
    right_output = tuple(col for col in CODE_UNIT_META_COLUMNS if col != key and col in right_cols)
    if not right_output:
        return None
    return JoinConfig.on_keys(
        keys=(key,),
        left_output=left_cols,
        right_output=right_output,
        output_suffix_for_right=output_suffix_for_right,
    )


def path_meta_config(
    left_columns: Sequence[str],
    right_columns: Sequence[str],
    *,
    key: str = "path",
    output_suffix_for_right: str = "",
) -> JoinConfig | None:
    """Return a JoinConfig for path metadata joins.

    Returns
    -------
    JoinConfig | None
        Join configuration or ``None`` when inputs are insufficient.
    """
    left_cols = tuple(left_columns)
    right_cols = tuple(right_columns)
    if key not in left_cols or key not in right_cols:
        return None
    right_output = tuple(col for col in PATH_META_COLUMNS if col != key and col in right_cols)
    if not right_output:
        return None
    return JoinConfig.on_keys(
        keys=(key,),
        left_output=left_cols,
        right_output=right_output,
        output_suffix_for_right=output_suffix_for_right,
    )


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


def interval_join_candidates(
    left: TableLike,
    right: TableLike,
    *,
    config: JoinConfig,
    join_type: JoinType = "inner",
    use_threads: bool = True,
) -> TableLike:
    """Return candidate interval matches using a join on path keys.

    Returns
    -------
    TableLike
        Joined table with left/right output selections.
    """
    spec = config.to_spec(join_type=join_type)
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


__all__ = [
    "CODE_UNIT_META_COLUMNS",
    "PATH_META_COLUMNS",
    "JoinConfig",
    "JoinOutputSpec",
    "code_unit_meta_config",
    "interval_join_candidates",
    "join_plan",
    "join_spec",
    "join_spec_for_keys",
    "left_join",
    "path_meta_config",
]
