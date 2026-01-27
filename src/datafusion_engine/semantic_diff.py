"""Semantic diff analysis for DataFusion plan changes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto


class ChangeCategory(Enum):
    """Categories of semantic changes."""

    NONE = auto()
    METADATA_ONLY = auto()
    ADDITIVE = auto()
    BREAKING = auto()
    ROW_MULTIPLYING = auto()


@dataclass(frozen=True)
class SemanticChange:
    """Represent a single semantic change between plan versions.

    Parameters
    ----------
    edit_type
        Type of edit: Insert, Remove, Update, Keep.
    node_type
        Logical plan node type or artifact label that changed.
    description
        Human-readable description of the change.
    category
        Semantic category of the change.
    """

    edit_type: str
    node_type: str
    description: str
    category: ChangeCategory


@dataclass
class SemanticDiff:
    """Semantic diff between two plan representations.

    Parameters
    ----------
    changes
        Categorized semantic changes.
    """

    changes: list[SemanticChange] = field(default_factory=list)

    @classmethod
    def compute(cls, old_plan: str | None, new_plan: str | None) -> SemanticDiff:
        """Compute a conservative semantic diff for plan representations.

        Parameters
        ----------
        old_plan
            Previous plan display or fingerprint.
        new_plan
            Updated plan display or fingerprint.

        Returns
        -------
        SemanticDiff
            Semantic diff object with categorized changes.
        """
        if old_plan == new_plan:
            return cls()
        return cls(
            changes=[
                SemanticChange(
                    edit_type="Update",
                    node_type="Plan",
                    description="Plan representation changed",
                    category=ChangeCategory.BREAKING,
                )
            ],
        )

    @property
    def overall_category(self) -> ChangeCategory:
        """Return the highest-severity category in the diff.

        Returns
        -------
        ChangeCategory
            Aggregate category derived from the change list.
        """
        if not self.changes:
            return ChangeCategory.NONE
        categories = {change.category for change in self.changes}
        if ChangeCategory.ROW_MULTIPLYING in categories:
            return ChangeCategory.ROW_MULTIPLYING
        if ChangeCategory.BREAKING in categories:
            return ChangeCategory.BREAKING
        if ChangeCategory.ADDITIVE in categories:
            return ChangeCategory.ADDITIVE
        if ChangeCategory.METADATA_ONLY in categories:
            return ChangeCategory.METADATA_ONLY
        return ChangeCategory.NONE

    def is_breaking(self) -> bool:
        """Check if diff contains breaking changes.

        Returns
        -------
        bool
            True if changes include breaking or row-multiplying changes.
        """
        return self.overall_category in {
            ChangeCategory.BREAKING,
            ChangeCategory.ROW_MULTIPLYING,
        }

    def requires_rebuild(self, policy: RebuildPolicy) -> bool:
        """Check if diff requires downstream rebuild.

        Parameters
        ----------
        policy
            Rebuild decision policy.

        Returns
        -------
        bool
            True if rebuild is needed under the given policy.
        """
        if policy == RebuildPolicy.ALWAYS:
            return self.overall_category != ChangeCategory.NONE
        if policy == RebuildPolicy.BREAKING_ONLY:
            return self.is_breaking()
        if policy == RebuildPolicy.CONSERVATIVE:
            return self.overall_category not in {
                ChangeCategory.NONE,
                ChangeCategory.METADATA_ONLY,
            }
        return False

    def summary(self) -> str:
        """Generate human-readable summary of changes.

        Returns
        -------
        str
            Summary string describing the changes.
        """
        if self.overall_category == ChangeCategory.NONE:
            return "No semantic changes"
        by_type: dict[str, int] = {}
        for change in self.changes:
            if change.category != ChangeCategory.NONE:
                by_type[change.edit_type] = by_type.get(change.edit_type, 0) + 1
        parts = [f"{count} {typ}" for typ, count in by_type.items()]
        return f"Changes: {', '.join(parts)} ({self.overall_category.name})"


class RebuildPolicy(Enum):
    """Policies for when to trigger rebuilds."""

    ALWAYS = auto()
    BREAKING_ONLY = auto()
    CONSERVATIVE = auto()


def compute_rebuild_needed(
    old_sql: str,
    new_sql: str,
    *,
    profile: object | None = None,
    schema: object | None = None,
    policy: RebuildPolicy = RebuildPolicy.CONSERVATIVE,
) -> tuple[bool, SemanticDiff]:
    """Determine if plan change requires downstream rebuild.

    Parameters
    ----------
    old_sql
        Previous plan display or fingerprint.
    new_sql
        Updated plan display or fingerprint.
    profile
        Deprecated SQL policy input (unused).
    schema
        Deprecated schema input (unused).
    policy
        Rebuild decision policy.

    Returns
    -------
    tuple[bool, SemanticDiff]
        Whether rebuild is needed and the diff details.
    """
    _ = (profile, schema)
    diff_result = SemanticDiff.compute(old_sql, new_sql)
    return diff_result.requires_rebuild(policy), diff_result


__all__ = [
    "ChangeCategory",
    "RebuildPolicy",
    "SemanticChange",
    "SemanticDiff",
    "compute_rebuild_needed",
]
