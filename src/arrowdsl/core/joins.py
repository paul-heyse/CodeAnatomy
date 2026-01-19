"""Join helpers for table-based normalization paths."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import cast

from arrowdsl.core.interop import TableLike
from arrowdsl.core.plan_ops import AsofJoinSpec, JoinSpec, JoinType

CODE_UNIT_META_COLUMNS: tuple[str, ...] = ("code_unit_id", "file_id", "path")
PATH_META_COLUMNS: tuple[str, ...] = ("path", "file_id")


type _JoinFn = Callable[..., TableLike]


def _apply_join_fn() -> _JoinFn:
    module = importlib.import_module("arrowdsl.compute.kernels")
    return cast("_JoinFn", module.apply_join)


def _apply_asof_join_fn() -> _JoinFn:
    module = importlib.import_module("arrowdsl.compute.kernels")
    return cast("_JoinFn", module.apply_asof_join)


@dataclass(frozen=True)
class JoinOutputSpec:
    """Output column selections and suffixes for joins."""

    left_output: Sequence[str] = ()
    right_output: Sequence[str] = ()
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""


@dataclass(frozen=True)
class JoinPlan:
    """Join specification plus resolved output columns."""

    spec: JoinSpec
    output_names: tuple[str, ...]


def _resolve_outputs(
    joined_columns: Sequence[str],
    *,
    outputs: Sequence[str],
    suffix: str,
    prefer_suffix: bool,
) -> list[str]:
    resolved: list[str] = []
    joined_set = set(joined_columns)
    for name in outputs:
        if suffix and prefer_suffix:
            candidate = f"{name}{suffix}"
            if candidate in joined_set:
                resolved.append(candidate)
                continue
        if name in joined_set:
            resolved.append(name)
            continue
        if suffix and not prefer_suffix:
            candidate = f"{name}{suffix}"
            if candidate in joined_set:
                resolved.append(candidate)
    return resolved


def resolve_join_outputs(
    joined_columns: Sequence[str],
    *,
    spec: JoinSpec,
) -> JoinPlan:
    """Resolve join output column names with suffix handling.

    Returns
    -------
    JoinPlan
        Join plan with resolved output names.
    """
    if not spec.left_output and not spec.right_output:
        return JoinPlan(spec=spec, output_names=tuple(joined_columns))
    left_names = set(spec.left_output) | set(spec.left_keys)
    prefer_right_suffix = bool(spec.output_suffix_for_right) and any(
        name in left_names for name in spec.right_output
    )
    left_cols = _resolve_outputs(
        joined_columns,
        outputs=spec.left_output,
        suffix=spec.output_suffix_for_left,
        prefer_suffix=False,
    )
    right_cols = _resolve_outputs(
        joined_columns,
        outputs=spec.right_output,
        suffix=spec.output_suffix_for_right,
        prefer_suffix=prefer_right_suffix,
    )
    output = [*left_cols, *right_cols]
    return JoinPlan(spec=spec, output_names=tuple(output))


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


def join_config_for_output(
    *,
    left_columns: Sequence[str],
    right_columns: Sequence[str],
    key_pairs: Sequence[tuple[str, str]],
    right_output: Sequence[str],
    output_suffix_for_right: str = "",
) -> JoinConfig | None:
    """Return a JoinConfig for left joins with explicit right output.

    Returns
    -------
    JoinConfig | None
        Join configuration or ``None`` when inputs are insufficient.
    """
    if not key_pairs:
        return None
    left_cols = tuple(left_columns)
    right_cols = tuple(right_columns)
    left_key_seq = tuple(pair[0] for pair in key_pairs)
    right_key_seq = tuple(pair[1] for pair in key_pairs)
    if not set(left_key_seq).issubset(left_cols):
        return None
    if not set(right_key_seq).issubset(right_cols):
        return None
    right_out = tuple(col for col in right_output if col in right_cols)
    if not right_out:
        return None
    return JoinConfig.from_sequences(
        left_keys=left_key_seq,
        right_keys=right_key_seq,
        left_output=left_cols,
        right_output=right_out,
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


def left_join(
    left: TableLike,
    right: TableLike,
    *,
    config: JoinConfig,
    use_threads: bool = True,
) -> TableLike:
    """Perform a left outer join in the kernel lane.

    Returns
    -------
    TableLike
        Joined table.
    """
    spec = config.to_spec(join_type="left outer")
    apply_join = _apply_join_fn()
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
    apply_join = _apply_join_fn()
    return apply_join(left, right, spec=spec, use_threads=use_threads)


def asof_join(
    left: TableLike,
    right: TableLike,
    *,
    spec: AsofJoinSpec,
    use_threads: bool = True,
) -> TableLike:
    """Return an as-of join between two tables.

    Returns
    -------
    TableLike
        As-of joined table.
    """
    apply_asof_join = _apply_asof_join_fn()
    return apply_asof_join(left, right, spec=spec, use_threads=use_threads)


__all__ = [
    "CODE_UNIT_META_COLUMNS",
    "PATH_META_COLUMNS",
    "AsofJoinSpec",
    "JoinConfig",
    "JoinOutputSpec",
    "JoinPlan",
    "asof_join",
    "code_unit_meta_config",
    "interval_join_candidates",
    "join_config_for_output",
    "join_spec",
    "join_spec_for_keys",
    "left_join",
    "path_meta_config",
    "resolve_join_outputs",
]
