"""Semantic diff analysis for SQL query changes.

This module provides semantic change detection between SQL query versions using
SQLGlot's AST diff capabilities. It categorizes changes (additive, breaking,
row-multiplying) to enable fine-grained incremental rebuild decisions.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, cast

import sqlglot.diff as sqlglot_diff
import sqlglot.expressions as exp

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlglot.diff import Edit

    from datafusion_engine.sql_policy_engine import SQLPolicyProfile
    from sqlglot_tools.optimizer import SchemaMapping


class ChangeCategory(Enum):
    """Categories of semantic changes."""

    NONE = auto()  # No changes
    METADATA_ONLY = auto()  # Comments, formatting
    ADDITIVE = auto()  # New columns, no removals
    BREAKING = auto()  # Removals, type changes, join changes
    ROW_MULTIPLYING = auto()  # Changes that may affect row count


@dataclass(frozen=True)
class SemanticChange:
    """A single semantic change between query versions.

    Parameters
    ----------
    edit_type
        Type of edit: Insert, Remove, Move, Update, Keep.
    node_type
        SQLGlot expression type that changed.
    description
        Human-readable description of the change.
    category
        Semantic category of the change.
    """

    edit_type: str  # Insert, Remove, Move, Update, Keep
    node_type: str  # Expression type that changed
    description: str
    category: ChangeCategory

    @classmethod
    def from_edit(cls, edit: Edit) -> SemanticChange:
        """Create from SQLGlot diff edit.

        Parameters
        ----------
        edit
            SQLGlot diff edit object.

        Returns
        -------
        SemanticChange
            Categorized semantic change.
        """
        edit_type = type(edit).__name__

        if isinstance(edit, sqlglot_diff.Keep):
            return cls(
                edit_type=edit_type,
                node_type=type(edit.source).__name__,
                description="No change",
                category=ChangeCategory.NONE,
            )

        if isinstance(edit, sqlglot_diff.Insert):
            node = edit.expression
            category = cls._categorize_insert(node)
            return cls(
                edit_type=edit_type,
                node_type=type(node).__name__,
                description=f"Added {type(node).__name__}",
                category=category,
            )

        if isinstance(edit, sqlglot_diff.Remove):
            node = edit.expression
            return cls(
                edit_type=edit_type,
                node_type=type(node).__name__,
                description=f"Removed {type(node).__name__}",
                category=ChangeCategory.BREAKING,
            )

        if isinstance(edit, sqlglot_diff.Move):
            return cls(
                edit_type=edit_type,
                node_type=type(edit.source).__name__,
                description=f"Moved {type(edit.source).__name__}",
                category=ChangeCategory.BREAKING,
            )

        if isinstance(edit, sqlglot_diff.Update):
            return cls(
                edit_type=edit_type,
                node_type=type(edit.source).__name__,
                description=f"Updated {type(edit.source).__name__}",
                category=ChangeCategory.BREAKING,
            )

        return cls(
            edit_type=edit_type,
            node_type="Unknown",
            description="Unknown change",
            category=ChangeCategory.BREAKING,
        )

    @staticmethod
    def _categorize_insert(node: exp.Expression) -> ChangeCategory:
        """Categorize an insert based on what was added.

        Parameters
        ----------
        node
            SQLGlot expression that was inserted.

        Returns
        -------
        ChangeCategory
            Category based on semantic impact of the insert.
        """
        # Projections are typically additive
        if isinstance(node, (exp.Column, exp.Alias)):
            return ChangeCategory.ADDITIVE

        # UDTFs can multiply rows
        if isinstance(node, exp.Unnest):
            return ChangeCategory.ROW_MULTIPLYING

        # Joins can multiply rows
        if isinstance(node, exp.Join):
            return ChangeCategory.ROW_MULTIPLYING

        # Default to breaking for safety
        return ChangeCategory.BREAKING


@dataclass
class SemanticDiff:
    """Semantic diff between two query versions.

    Provides fine-grained change detection for incremental rebuild decisions.

    Parameters
    ----------
    edits
        Raw SQLGlot diff edits.
    changes
        Categorized semantic changes.
    """

    edits: list[Edit] = field(default_factory=list)
    changes: list[SemanticChange] = field(default_factory=list)

    @classmethod
    def compute(
        cls,
        old_ast: exp.Expression,
        new_ast: exp.Expression,
        *,
        matchings: list[tuple[exp.Expression, exp.Expression]] | None = None,
    ) -> SemanticDiff:
        """Compute semantic diff between two ASTs.

        Parameters
        ----------
        old_ast
            Previous query AST.
        new_ast
            New query AST.
        matchings
            Optional pre-matched node pairs to guide diff.

        Returns
        -------
        SemanticDiff
            Diff result with categorized changes.
        """
        seeded = matchings or _seed_matchings(old_ast, new_ast)
        edits = sqlglot_diff.diff(old_ast, new_ast, matchings=seeded)
        changes = [SemanticChange.from_edit(edit) for edit in edits]

        return cls(edits=edits, changes=changes)

    @property
    def overall_category(self) -> ChangeCategory:
        """Get the most severe change category.

        Returns
        -------
        ChangeCategory
            Highest severity category among all changes.
        """
        categories = [c.category for c in self.changes]

        if not categories or all(c == ChangeCategory.NONE for c in categories):
            return ChangeCategory.NONE

        if ChangeCategory.ROW_MULTIPLYING in categories:
            return ChangeCategory.ROW_MULTIPLYING

        if ChangeCategory.BREAKING in categories:
            return ChangeCategory.BREAKING

        if ChangeCategory.ADDITIVE in categories:
            return ChangeCategory.ADDITIVE

        return ChangeCategory.METADATA_ONLY

    def is_breaking(self: SemanticDiff) -> bool:
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

    def requires_rebuild(self: SemanticDiff, policy: RebuildPolicy) -> bool:
        """Check if diff requires downstream rebuild.

        Policy determines what counts as "requiring rebuild".

        Parameters
        ----------
        self
            Semantic diff instance.
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

    def summary(self: SemanticDiff) -> str:
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


def _seed_matchings(
    old_ast: exp.Expression,
    new_ast: exp.Expression,
) -> list[tuple[exp.Expression, exp.Expression]]:
    """Seed diff matchings using stable identifiers for common nodes.

    Returns
    -------
    list[tuple[sqlglot.expressions.Expression, sqlglot.expressions.Expression]]
        Seeded node matchings for diff stabilization.
    """
    old_index = _match_index(old_ast)
    new_index = _match_index(new_ast)
    matchings: list[tuple[exp.Expression, exp.Expression]] = []
    for key, old_nodes in old_index.items():
        new_nodes = new_index.get(key)
        if new_nodes is None or len(old_nodes) != 1 or len(new_nodes) != 1:
            continue
        matchings.append((old_nodes[0], new_nodes[0]))
    return matchings


def _match_index(ast: exp.Expression) -> dict[tuple[str, str], list[exp.Expression]]:
    """Return matchable node indices keyed by stable identifiers.

    Returns
    -------
    dict[tuple[str, str], list[sqlglot.expressions.Expression]]
        Matchable nodes grouped by stable identifiers.
    """
    index: dict[tuple[str, str], list[exp.Expression]] = {}
    for node in ast.walk():
        key = _match_key(node)
        if key is None:
            continue
        index.setdefault(key, []).append(node)
    return index


def _match_key(node: exp.Expression) -> tuple[str, str] | None:
    def _column_key(col: exp.Column) -> tuple[str, str] | None:
        name = col.name
        if not name:
            return None
        table = col.table or ""
        return ("column", f"{table}.{name}" if table else name)

    matchers: list[
        tuple[type[exp.Expression], Callable[[exp.Expression], tuple[str, str] | None]]
    ] = [
        (
            exp.Table,
            lambda n: ("table", n.name) if n.name else None,
        ),
        (
            exp.Column,
            lambda n: _column_key(cast("exp.Column", n)),
        ),
        (
            exp.Alias,
            lambda n: ("alias", n.alias_or_name) if n.alias_or_name else None,
        ),
        (
            exp.CTE,
            lambda n: ("cte", n.alias_or_name) if n.alias_or_name else None,
        ),
        (
            exp.Subquery,
            lambda n: ("subquery", n.alias_or_name) if n.alias_or_name else None,
        ),
    ]
    for node_type, matcher in matchers:
        if isinstance(node, node_type):
            return matcher(node)
    return None


class RebuildPolicy(Enum):
    """Policies for when to trigger rebuilds."""

    ALWAYS = auto()  # Rebuild on any change
    BREAKING_ONLY = auto()  # Rebuild only on breaking changes
    CONSERVATIVE = auto()  # Rebuild unless purely metadata


def compute_rebuild_needed(
    old_sql: str,
    new_sql: str,
    *,
    profile: SQLPolicyProfile,
    schema: SchemaMapping,
    policy: RebuildPolicy = RebuildPolicy.CONSERVATIVE,
) -> tuple[bool, SemanticDiff]:
    """Determine if SQL change requires downstream rebuild.

    Parameters
    ----------
    old_sql
        Previous SQL query.
    new_sql
        New SQL query.
    profile
        SQL policy for canonicalization.
    schema
        Schema for qualification.
    policy
        Rebuild decision policy.

    Returns
    -------
    tuple[bool, SemanticDiff]
        Whether rebuild is needed and the diff details.
    """
    from datafusion_engine.sql_policy_engine import compile_sql_policy

    # Parse and canonicalize both versions
    from sqlglot_tools.optimizer import StrictParseOptions, parse_sql_strict

    options = StrictParseOptions(
        error_level=profile.error_level,
        unsupported_level=profile.unsupported_level,
    )
    dialect = profile.read_dialect or "datafusion"
    old_ast = parse_sql_strict(old_sql, dialect=dialect, options=options)
    new_ast = parse_sql_strict(new_sql, dialect=dialect, options=options)

    old_canonical, _ = compile_sql_policy(old_ast, schema=schema, profile=profile)
    new_canonical, _ = compile_sql_policy(new_ast, schema=schema, profile=profile)

    # Compute diff on canonical forms
    diff_result = SemanticDiff.compute(old_canonical, new_canonical)

    return diff_result.requires_rebuild(policy), diff_result
