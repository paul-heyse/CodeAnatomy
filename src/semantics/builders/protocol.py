"""Builder protocol for semantic views.

Defines the protocol that all semantic view builders must implement.
The protocol ensures consistent metadata (name, evidence tier, upstream
dependencies) and a standard build method signature.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext


@runtime_checkable
class SemanticViewBuilder(Protocol):
    """Protocol for semantic view builders.

    Semantic view builders construct DataFusion DataFrames that represent
    semantic views in the CPG. Each builder provides metadata about the view
    it produces including its name, evidence tier, and upstream dependencies.

    Evidence Tier Semantics
    -----------------------
    Tiers indicate reliability/confidence of evidence:
    - Tier 1: Syntactic (CST/AST) - high confidence
    - Tier 2: Semantic (symtable, bytecode) - medium confidence
    - Tier 3: External (SCIP, type inference) - variable confidence
    - Tier 4: Derived/computed - depends on inputs

    Attributes
    ----------
    name : str
        Unique name for the view.
    evidence_tier : int
        Reliability tier (1=highest confidence, 4=lowest).
    upstream_deps : tuple[str, ...]
        Names of upstream tables/views this builder requires.

    Methods
    -------
    build(ctx)
        Construct the DataFrame for this view.
    """

    @property
    def name(self) -> str:
        """Return the unique view name.

        Returns
        -------
        str
            View name used for registration and dependency tracking.
        """
        ...

    @property
    def evidence_tier(self) -> int:
        """Return the evidence tier.

        Returns
        -------
        int
            Evidence tier (1-4, lower is more reliable).
        """
        ...

    @property
    def upstream_deps(self) -> tuple[str, ...]:
        """Return the names of upstream dependencies.

        Returns
        -------
        tuple[str, ...]
            Names of tables/views this builder depends on.
        """
        ...

    def build(self, ctx: SessionContext) -> DataFrame:
        """Build the view DataFrame.

        Parameters
        ----------
        ctx
            DataFusion session context with registered upstream tables.

        Returns
        -------
        DataFrame
            The constructed view DataFrame.
        """
        ...


__all__ = ["SemanticViewBuilder"]
