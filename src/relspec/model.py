"""Relationship rule models and configuration dataclasses."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.core.plan_ops import DedupeSpec, JoinType, SortKey
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.query_compiler import IbisQuerySpec

type Expression = ExprIR
type ExecutionMode = Literal["auto", "plan", "table", "external", "hybrid"]

HASH_JOIN_INPUTS = 2
SINGLE_INPUT = 1


@dataclass(frozen=True)
class DatasetRef:
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

    name: str
    query: IbisQuerySpec | None = None
    label: str = ""


class RuleKind(StrEnum):
    """Relationship rule kinds for plan or kernel lanes."""

    FILTER_PROJECT = "filter_project"
    HASH_JOIN = "hash_join"
    UNION_ALL = "union_all"
    INTERVAL_ALIGN = "interval_align"
    EXPLODE_LIST = "explode_list"
    WINNER_SELECT = "winner_select"


@dataclass(frozen=True)
class HashJoinConfig:
    """Hash join configuration.

    Parameters
    ----------
    join_type:
        Join type string.
    left_keys:
        Left-side join keys.
    right_keys:
        Right-side join keys.
    left_output:
        Output columns from the left side, or ``None`` for all columns.
    right_output:
        Output columns from the right side, or ``None`` for all columns.
    output_suffix_for_left:
        Suffix for left output column collisions.
    output_suffix_for_right:
        Suffix for right output column collisions.
    """

    join_type: JoinType = "inner"
    left_keys: tuple[str, ...] = ()
    right_keys: tuple[str, ...] = ()
    left_output: tuple[str, ...] | None = None
    right_output: tuple[str, ...] | None = None
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""

    def resolved_right_keys(self) -> tuple[str, ...]:
        """Return the right-hand join keys (fallback to left keys).

        Returns
        -------
        tuple[str, ...]
            Join keys for the right-hand side.
        """
        return self.right_keys or self.left_keys


@dataclass(frozen=True)
class IntervalAlignConfig:
    """Kernel-lane interval alignment configuration."""

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


@dataclass(frozen=True)
class WinnerSelectConfig:
    """Kernel-lane winner selection configuration."""

    keys: tuple[str, ...] = ()
    score_col: str = "score"
    score_order: Literal["ascending", "descending"] = "descending"
    tie_breakers: tuple[SortKey, ...] = ()


@dataclass(frozen=True)
class EvidenceSpec:
    """Evidence requirements for a relationship rule."""

    sources: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    required_types: Mapping[str, str] = field(default_factory=dict)
    required_metadata: Mapping[bytes, bytes] = field(default_factory=dict)

    def resolved_sources(self, inputs: Sequence[DatasetRef]) -> tuple[str, ...]:
        """Return evidence sources or fall back to rule inputs.

        Parameters
        ----------
        inputs:
            Dataset references for the rule inputs.

        Returns
        -------
        tuple[str, ...]
            Evidence sources for the rule.
        """
        if self.sources:
            return self.sources
        return tuple(ref.name for ref in inputs)


@dataclass(frozen=True)
class ConfidencePolicy:
    """Policy for computing confidence scores."""

    base: float = 0.5
    source_weight: Mapping[str, float] = field(default_factory=dict)
    penalty: float = 0.0


@dataclass(frozen=True)
class AmbiguityPolicy:
    """Policy for ambiguity resolution."""

    winner_select: WinnerSelectConfig | None = None
    tie_breakers: tuple[SortKey, ...] = ()


@dataclass(frozen=True)
class RuleFamilySpec:
    """Declarative specification for a relationship rule family."""

    name: str
    factory: str
    inputs: tuple[str, ...] = ()
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    option_flag: str | None = None


@dataclass(frozen=True)
class ProjectConfig:
    """Projection performed after the primary operation."""

    select: tuple[str, ...] = ()
    exprs: Mapping[str, Expression] = field(default_factory=dict)


@dataclass(frozen=True)
class KernelSpec:
    """Base class for post-kernel specifications."""

    kind: str


@dataclass(frozen=True)
class AddLiteralSpec(KernelSpec):
    """Post-kernel spec for adding a literal column."""

    kind: Literal["add_literal"] = "add_literal"
    name: str = ""
    value: ScalarValue | None = None


@dataclass(frozen=True)
class DropColumnsSpec(KernelSpec):
    """Post-kernel spec for dropping columns."""

    kind: Literal["drop_columns"] = "drop_columns"
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class FilterKernelSpec(KernelSpec):
    """Post-kernel spec for filtering rows."""

    predicate: Expression = field(default_factory=lambda: ExprIR(op="literal", value=True))
    kind: Literal["filter"] = "filter"


@dataclass(frozen=True)
class RenameColumnsSpec(KernelSpec):
    """Post-kernel spec for renaming columns."""

    kind: Literal["rename_columns"] = "rename_columns"
    mapping: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExplodeListSpec(KernelSpec):
    """Post-kernel spec for exploding list columns."""

    kind: Literal["explode_list"] = "explode_list"
    parent_id_col: str = "src_id"
    list_col: str = "dst_ids"
    out_parent_col: str = "src_id"
    out_value_col: str = "dst_id"
    idx_col: str | None = "idx"
    keep_empty: bool = True


@dataclass(frozen=True)
class DedupeKernelSpec(KernelSpec):
    """Post-kernel spec for applying deduplication."""

    kind: Literal["dedupe"] = "dedupe"
    spec: DedupeSpec = field(default_factory=lambda: DedupeSpec(keys=()))


@dataclass(frozen=True)
class CanonicalSortKernelSpec(KernelSpec):
    """Post-kernel spec for applying canonical sorting."""

    kind: Literal["canonical_sort"] = "canonical_sort"
    sort_keys: tuple[SortKey, ...] = ()


type KernelSpecT = (
    AddLiteralSpec
    | DropColumnsSpec
    | FilterKernelSpec
    | RenameColumnsSpec
    | ExplodeListSpec
    | DedupeKernelSpec
    | CanonicalSortKernelSpec
)


@dataclass(frozen=True)
class RelationshipRule:
    """Declarative relationship rule configuration."""

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
    execution_mode: ExecutionMode = "auto"
    evidence: EvidenceSpec | None = None
    confidence_policy: ConfidencePolicy | None = None
    ambiguity_policy: AmbiguityPolicy | None = None

    def __post_init__(self) -> None:
        """Validate relationship rule invariants."""
        self._validate_rule()

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

        self._validate_variant_configs()
        self._validate_kind_inputs()

    def _validate_variant_configs(self) -> None:
        """Validate that only the active rule variant config is set."""
        variant_configs = {
            RuleKind.HASH_JOIN: self.hash_join,
            RuleKind.INTERVAL_ALIGN: self.interval_align,
            RuleKind.WINNER_SELECT: self.winner_select,
        }
        active_variants = {kind for kind, config in variant_configs.items() if config is not None}
        if self.kind in variant_configs:
            self._require_variant_config(active_variants)
            return
        self._forbid_variant_configs(active_variants)

    def _require_variant_config(self, active_variants: set[RuleKind]) -> None:
        """Ensure the active rule kind has exactly one config set.

        Parameters
        ----------
        active_variants
            Rule kinds with non-null configs.

        Raises
        ------
        ValueError
            Raised when required variant configs are missing or conflicting.
        """
        if self.kind not in active_variants:
            msg = f"{self.kind.name} rules require {self.kind.value} config."
            raise ValueError(msg)
        if len(active_variants) != 1:
            extras = sorted(kind.value for kind in active_variants if kind != self.kind)
            msg = f"{self.kind.name} rules cannot mix configs: {extras}."
            raise ValueError(msg)

    def _forbid_variant_configs(self, active_variants: set[RuleKind]) -> None:
        """Reject variant configs for rule kinds that do not require them.

        Parameters
        ----------
        active_variants
            Rule kinds with non-null configs.

        Raises
        ------
        ValueError
            Raised when variant configs are present for the current rule kind.
        """
        if not active_variants:
            return
        extras = sorted(kind.value for kind in active_variants)
        msg = f"{self.kind.name} rules cannot set configs: {extras}."
        raise ValueError(msg)

    def _validate_kind_inputs(self) -> None:
        """Validate input arity requirements for the configured rule kind."""
        validators: dict[RuleKind, Callable[[], None]] = {
            RuleKind.HASH_JOIN: self._validate_hash_join,
            RuleKind.FILTER_PROJECT: lambda: self._require_exact_inputs(SINGLE_INPUT),
            RuleKind.EXPLODE_LIST: lambda: self._require_exact_inputs(SINGLE_INPUT),
            RuleKind.WINNER_SELECT: self._validate_winner_select,
            RuleKind.UNION_ALL: lambda: self._require_min_inputs(1),
            RuleKind.INTERVAL_ALIGN: self._validate_interval_align,
        }
        validator = validators.get(self.kind)
        if validator is not None:
            validator()

    def _require_exact_inputs(self, count: int) -> None:
        """Ensure the rule has exactly ``count`` inputs.

        Parameters
        ----------
        count
            Required number of inputs.

        Raises
        ------
        ValueError
            Raised when the rule has a different number of inputs.
        """
        if len(self.inputs) != count:
            msg = f"{self.kind.name} rules require exactly {count} inputs."
            raise ValueError(msg)

    def _require_min_inputs(self, count: int) -> None:
        """Ensure the rule has at least ``count`` inputs.

        Parameters
        ----------
        count
            Minimum number of inputs.

        Raises
        ------
        ValueError
            Raised when the rule has fewer than the required inputs.
        """
        if len(self.inputs) < count:
            msg = f"{self.kind.name} rules require at least {count} inputs."
            raise ValueError(msg)

    def _validate_hash_join(self) -> None:
        """Validate hash-join rule inputs and configuration.

        Raises
        ------
        ValueError
            Raised when hash-join configuration requirements are not met.
        """
        self._require_exact_inputs(HASH_JOIN_INPUTS)
        if self.hash_join is None:
            msg = "HASH_JOIN rules require hash_join config."
            raise ValueError(msg)

    def _validate_interval_align(self) -> None:
        """Validate interval-align rule inputs and configuration.

        Raises
        ------
        ValueError
            Raised when interval-align configuration requirements are not met.
        """
        self._require_exact_inputs(HASH_JOIN_INPUTS)
        if self.interval_align is None:
            msg = "INTERVAL_ALIGN rules require interval_align config."
            raise ValueError(msg)

    def _validate_winner_select(self) -> None:
        """Validate winner-select rule inputs and configuration.

        Raises
        ------
        ValueError
            Raised when winner-select configuration requirements are not met.
        """
        self._require_exact_inputs(SINGLE_INPUT)
        if self.winner_select is None:
            msg = "WINNER_SELECT rules require winner_select config."
            raise ValueError(msg)
