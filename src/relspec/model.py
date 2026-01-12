"""Relationship rule models and configuration dataclasses."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from arrowdsl.compute.expr import ScalarValue
from arrowdsl.core.interop import ComputeExpression
from arrowdsl.plan.ops import DedupeSpec, JoinSpec, JoinType, SortKey
from arrowdsl.plan.query import QuerySpec

type Expression = ComputeExpression

HASH_JOIN_INPUTS = 2
SINGLE_INPUT = 1


class DatasetRef(BaseModel):
    """Reference a dataset by registry name.

    Parameters
    ----------
    name:
        Registry name for the dataset.
    query:
        Optional query specification.
    label:
        Optional plan label override.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    name: str
    query: QuerySpec | None = None
    label: str = ""


class RuleKind(StrEnum):
    """Relationship rule kinds for plan or kernel lanes."""

    FILTER_PROJECT = "filter_project"
    HASH_JOIN = "hash_join"
    UNION_ALL = "union_all"
    INTERVAL_ALIGN = "interval_align"
    EXPLODE_LIST = "explode_list"
    WINNER_SELECT = "winner_select"


class HashJoinConfig(BaseModel):
    """Acero HashJoin node configuration.

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
        Suffix for left output column collisions.
    output_suffix_for_right:
        Suffix for right output column collisions.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    join_type: JoinType = "inner"
    left_keys: tuple[str, ...] = ()
    right_keys: tuple[str, ...] = ()
    left_output: tuple[str, ...] = ()
    right_output: tuple[str, ...] = ()
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""

    def to_join_spec(self) -> JoinSpec:
        """Convert the config into a JoinSpec.

        Returns
        -------
        JoinSpec
            Join specification for hash joins.
        """
        right_keys = self.right_keys or self.left_keys
        return JoinSpec(
            join_type=self.join_type,
            left_keys=self.left_keys,
            right_keys=right_keys,
            left_output=self.left_output,
            right_output=self.right_output,
            output_suffix_for_left=self.output_suffix_for_left,
            output_suffix_for_right=self.output_suffix_for_right,
        )


class IntervalAlignConfig(BaseModel):
    """Kernel-lane interval alignment configuration."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    mode: Literal["EXACT", "CONTAINED_BEST", "OVERLAP_BEST"] = "CONTAINED_BEST"
    how: Literal["inner", "left"] = "inner"

    left_path_col: str = "path"
    left_start_col: str = "bstart"
    left_end_col: str = "bend"

    right_path_col: str = "path"
    right_start_col: str = "bstart"
    right_end_col: str = "bend"

    select_left: tuple[str, ...] = ()
    select_right: tuple[str, ...] = ()

    tie_breakers: tuple[SortKey, ...] = ()

    emit_match_meta: bool = True
    match_kind_col: str = "match_kind"
    match_score_col: str = "match_score"


class WinnerSelectConfig(BaseModel):
    """Kernel-lane winner selection configuration."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    keys: tuple[str, ...] = ()
    score_col: str = "score"
    score_order: Literal["ascending", "descending"] = "descending"
    tie_breakers: tuple[SortKey, ...] = ()


class ProjectConfig(BaseModel):
    """Projection performed after the primary operation."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    select: tuple[str, ...] = ()
    exprs: Mapping[str, Expression] = Field(default_factory=dict)


class KernelSpec(BaseModel):
    """Base class for post-kernel specifications."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    kind: str


class AddLiteralSpec(KernelSpec):
    """Post-kernel spec for adding a literal column."""

    kind: Literal["add_literal"] = "add_literal"
    name: str = ""
    value: ScalarValue | None = None


class DropColumnsSpec(KernelSpec):
    """Post-kernel spec for dropping columns."""

    kind: Literal["drop_columns"] = "drop_columns"
    columns: tuple[str, ...] = ()


class RenameColumnsSpec(KernelSpec):
    """Post-kernel spec for renaming columns."""

    kind: Literal["rename_columns"] = "rename_columns"
    mapping: Mapping[str, str] = Field(default_factory=dict)


class ExplodeListSpec(KernelSpec):
    """Post-kernel spec for exploding list columns."""

    kind: Literal["explode_list"] = "explode_list"
    parent_id_col: str = "src_id"
    list_col: str = "dst_ids"
    out_parent_col: str = "src_id"
    out_value_col: str = "dst_id"


class DedupeKernelSpec(KernelSpec):
    """Post-kernel spec for applying deduplication."""

    kind: Literal["dedupe"] = "dedupe"
    spec: DedupeSpec = Field(default_factory=lambda: DedupeSpec(keys=()))


class CanonicalSortKernelSpec(KernelSpec):
    """Post-kernel spec for applying canonical sorting."""

    kind: Literal["canonical_sort"] = "canonical_sort"
    sort_keys: tuple[SortKey, ...] = ()


type KernelSpecT = (
    AddLiteralSpec
    | DropColumnsSpec
    | RenameColumnsSpec
    | ExplodeListSpec
    | DedupeKernelSpec
    | CanonicalSortKernelSpec
)


class RelationshipRule(BaseModel):
    """Declarative relationship rule configuration."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    name: str
    kind: RuleKind
    output_dataset: str
    contract_name: str | None = None

    inputs: tuple[DatasetRef, ...] = ()

    hash_join: HashJoinConfig | None = None
    interval_align: IntervalAlignConfig | None = None
    winner_select: WinnerSelectConfig | None = None

    project: ProjectConfig | None = None
    post_kernels: tuple[KernelSpecT, ...] = ()

    priority: int = 100
    emit_rule_meta: bool = True
    rule_name_col: str = "rule_name"
    rule_priority_col: str = "rule_priority"

    @model_validator(mode="after")
    def _validate_model(self) -> RelationshipRule:
        self._validate_rule()
        return self

    def _validate_rule(self) -> None:
        """Validate rule configuration invariants.

        Raises
        ------
        ValueError
            Raised when required rule configuration is missing or inconsistent.
        """
        if not self.name:
            msg = "RelationshipRule.name must be non-empty."
            raise ValueError(msg)
        if not self.output_dataset:
            msg = "RelationshipRule.output_dataset must be non-empty."
            raise ValueError(msg)

        if self.kind == RuleKind.HASH_JOIN:
            self._validate_hash_join()
            return
        if self.kind == RuleKind.FILTER_PROJECT:
            self._require_exact_inputs(SINGLE_INPUT)
            return
        if self.kind == RuleKind.EXPLODE_LIST:
            self._require_exact_inputs(SINGLE_INPUT)
            return
        if self.kind == RuleKind.WINNER_SELECT:
            self._validate_winner_select()
            return
        if self.kind == RuleKind.UNION_ALL:
            self._require_min_inputs(1)
            return
        if self.kind == RuleKind.INTERVAL_ALIGN:
            self._validate_interval_align()
            return

    def _require_exact_inputs(self, count: int) -> None:
        if len(self.inputs) != count:
            msg = f"{self.kind.name} rules require exactly {count} inputs."
            raise ValueError(msg)

    def _require_min_inputs(self, count: int) -> None:
        if len(self.inputs) < count:
            msg = f"{self.kind.name} rules require at least {count} inputs."
            raise ValueError(msg)

    def _validate_hash_join(self) -> None:
        self._require_exact_inputs(HASH_JOIN_INPUTS)
        if self.hash_join is None:
            msg = "HASH_JOIN rules require hash_join config."
            raise ValueError(msg)

    def _validate_interval_align(self) -> None:
        self._require_exact_inputs(HASH_JOIN_INPUTS)
        if self.interval_align is None:
            msg = "INTERVAL_ALIGN rules require interval_align config."
            raise ValueError(msg)

    def _validate_winner_select(self) -> None:
        self._require_exact_inputs(SINGLE_INPUT)
        if self.winner_select is None:
            msg = "WINNER_SELECT rules require winner_select config."
            raise ValueError(msg)
