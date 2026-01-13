"""Join spec factories for plan and kernel lanes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.plan.ops import JoinSpec, JoinType


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


__all__ = ["JoinOutputSpec", "join_spec", "join_spec_for_keys"]
