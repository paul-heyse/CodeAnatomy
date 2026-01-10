from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Union

from ..arrowdsl.contracts import DedupeSpec, SortKey

if TYPE_CHECKING:  # pragma: no cover
    from ..arrowdsl.queryspec import QuerySpec


Expression = Any  # pyarrow.compute.Expression (kept loose to avoid import-time pyarrow dependency)


@dataclass(frozen=True)
class DatasetRef:
    """
    Reference to a dataset by registry name.

    query:
      Optional QuerySpec. If omitted, the resolver decides how to scan (often "all columns").
      For production, you typically provide a QuerySpec to force projection/pushdown.

    label:
      Optional override for plan labeling/debug.
    """

    name: str
    query: QuerySpec | None = None
    label: str = ""


class RuleKind(str, Enum):
    """
    Relationship rule kinds.

    Plan-lane:
      - FILTER_PROJECT
      - HASH_JOIN

    Kernel-lane / composite:
      - UNION_ALL
      - INTERVAL_ALIGN
      - EXPLODE_LIST
      - WINNER_SELECT (dedupe)
    """

    FILTER_PROJECT = "filter_project"
    HASH_JOIN = "hash_join"
    UNION_ALL = "union_all"
    INTERVAL_ALIGN = "interval_align"
    EXPLODE_LIST = "explode_list"
    WINNER_SELECT = "winner_select"


@dataclass(frozen=True)
class HashJoinConfig:
    """
    Acero HashJoin node configuration.

    join_type examples:
      "inner", "left outer", "right outer", "full outer", "left semi", "left anti"
    """

    join_type: str = "inner"
    left_keys: tuple[str, ...] = ()
    right_keys: tuple[str, ...] = ()

    # Which columns are emitted from each side by the join node
    left_output: tuple[str, ...] = ()
    right_output: tuple[str, ...] = ()

    # If collisions happen, specify suffixes
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""


@dataclass(frozen=True)
class IntervalAlignConfig:
    """
    Kernel-lane interval alignment (span join).

    This is intended for joins like:
      CST name ref/callee token span  <->  SCIP occurrence range/enclosing_range
      CST def name span              <->  SCIP definition occurrence

    mode:
      - EXACT: right span must match left span exactly
      - CONTAINED_BEST: right span must be contained within left span; choose "best" candidate
      - OVERLAP_BEST: allow overlaps; choose best overlap (more permissive)

    "Best" defaults to:
      1) minimal right span length (more specific is better)
      2) tie_breakers (SortKey columns from right row)
    """

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

    # Tie-breaking columns from right rows (after span-length)
    tie_breakers: tuple[SortKey, ...] = ()

    # Optional match metadata columns
    emit_match_meta: bool = True
    match_kind_col: str = "match_kind"
    match_score_col: str = "match_score"  # numeric "better" score (higher is better)


@dataclass(frozen=True)
class ProjectConfig:
    """
    Projection performed after the primary operation.

    select:
      Output columns to keep (existing fields).
      If empty, defaults to "keep everything produced by the operation".

    exprs:
      New computed columns as {name: Expression}.
    """

    select: tuple[str, ...] = ()
    exprs: Mapping[str, Expression] = field(default_factory=dict)


# -------------------------
# Post-kernel specs
# -------------------------


@dataclass(frozen=True)
class KernelSpec:
    kind: str


@dataclass(frozen=True)
class AddLiteralSpec(KernelSpec):
    kind: Literal["add_literal"] = "add_literal"
    name: str = ""
    value: Any = None


@dataclass(frozen=True)
class DropColumnsSpec(KernelSpec):
    kind: Literal["drop_columns"] = "drop_columns"
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class RenameColumnsSpec(KernelSpec):
    kind: Literal["rename_columns"] = "rename_columns"
    mapping: Mapping[str, str] = field(default_factory=dict)  # old -> new


@dataclass(frozen=True)
class ExplodeListSpec(KernelSpec):
    kind: Literal["explode_list"] = "explode_list"
    parent_id_col: str = "src_id"
    list_col: str = "dst_ids"
    out_parent_col: str = "src_id"
    out_value_col: str = "dst_id"


@dataclass(frozen=True)
class DedupeKernelSpec(KernelSpec):
    kind: Literal["dedupe"] = "dedupe"
    spec: DedupeSpec = DedupeSpec(keys=())


@dataclass(frozen=True)
class CanonicalSortKernelSpec(KernelSpec):
    kind: Literal["canonical_sort"] = "canonical_sort"
    sort_keys: tuple[SortKey, ...] = ()


KernelSpecT = Union[
    AddLiteralSpec,
    DropColumnsSpec,
    RenameColumnsSpec,
    ExplodeListSpec,
    DedupeKernelSpec,
    CanonicalSortKernelSpec,
]


@dataclass(frozen=True)
class RelationshipRule:
    """
    A declarative relationship rule.

    Output policy:
      - output_dataset: dataset name this rule produces
      - contract_name: which Contract to finalize against (often relationship-specific)

    Determinism / winner selection:
      - priority: smaller is "preferred" (e.g., exact match priority 0, fallback priority 10)
      - if emit_rule_meta=True, compiler will add rule_name + rule_priority columns.
        You should include these in contracts if you want them persisted.
    """

    name: str
    kind: RuleKind
    output_dataset: str
    contract_name: str | None = None

    inputs: tuple[DatasetRef, ...] = ()

    # kind-specific configs
    hash_join: HashJoinConfig | None = None
    interval_align: IntervalAlignConfig | None = None

    project: ProjectConfig | None = None
    post_kernels: tuple[KernelSpecT, ...] = ()

    # meta/winner selection
    priority: int = 100
    emit_rule_meta: bool = True
    rule_name_col: str = "rule_name"
    rule_priority_col: str = "rule_priority"

    def validate(self) -> None:
        if not self.name:
            raise ValueError("RelationshipRule.name must be non-empty")
        if not self.output_dataset:
            raise ValueError("RelationshipRule.output_dataset must be non-empty")

        if self.kind == RuleKind.HASH_JOIN:
            if len(self.inputs) != 2:
                raise ValueError("HASH_JOIN rules require exactly 2 inputs")
            if self.hash_join is None:
                raise ValueError("HASH_JOIN rules require hash_join config")

        if self.kind == RuleKind.FILTER_PROJECT:
            if len(self.inputs) != 1:
                raise ValueError("FILTER_PROJECT rules require exactly 1 input")

        if self.kind == RuleKind.EXPLODE_LIST:
            if len(self.inputs) != 1:
                raise ValueError("EXPLODE_LIST rules require exactly 1 input")

        if self.kind == RuleKind.WINNER_SELECT:
            if len(self.inputs) != 1:
                raise ValueError("WINNER_SELECT rules require exactly 1 input")

        if self.kind == RuleKind.UNION_ALL:
            if len(self.inputs) < 1:
                raise ValueError("UNION_ALL rules require >= 1 input")

        if self.kind == RuleKind.INTERVAL_ALIGN:
            if len(self.inputs) != 2:
                raise ValueError("INTERVAL_ALIGN rules require exactly 2 inputs")
            if self.interval_align is None:
                raise ValueError("INTERVAL_ALIGN rules require interval_align config")
