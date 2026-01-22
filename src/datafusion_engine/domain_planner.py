"""Domain planner specifications for DataFusion expression planners."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DomainPlannerSpec:
    """Specification for a domain expression planner.

    Domain planners enable custom operator syntax and named argument support
    for domain-specific query patterns.
    """

    name: str
    operator_names: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        """Return a payload representation for the domain planner.

        Returns
        -------
        dict[str, object]
            Domain planner payload mapping.
        """
        return {
            "name": self.name,
            "operator_names": list(self.operator_names),
        }


CODEANATOMY_DOMAIN_PLANNER = DomainPlannerSpec(
    name="codeanatomy_domain",
    operator_names=(
        "stable_hash64",
        "stable_hash128",
        "prefixed_hash64",
        "stable_id",
        "col_to_byte",
    ),
)


DEFAULT_DOMAIN_PLANNERS: dict[str, DomainPlannerSpec] = {
    "codeanatomy_domain": CODEANATOMY_DOMAIN_PLANNER,
}


def resolve_domain_planner(name: str) -> DomainPlannerSpec | None:
    """Resolve a domain planner spec by name.

    Parameters
    ----------
    name
        Domain planner name.

    Returns
    -------
    DomainPlannerSpec | None
        Domain planner spec when available.
    """
    return DEFAULT_DOMAIN_PLANNERS.get(name)


def domain_planner_payloads(planner_names: tuple[str, ...]) -> list[dict[str, object]]:
    """Return structured payloads for domain planners.

    Parameters
    ----------
    planner_names
        Tuple of domain planner names to resolve.

    Returns
    -------
    list[dict[str, object]]
        Payload representations for each resolved planner.
    """
    payloads: list[dict[str, object]] = []
    for name in planner_names:
        planner = resolve_domain_planner(name)
        if planner is not None:
            payloads.append(planner.to_payload())
    return payloads


__all__ = [
    "CODEANATOMY_DOMAIN_PLANNER",
    "DEFAULT_DOMAIN_PLANNERS",
    "DomainPlannerSpec",
    "domain_planner_payloads",
    "resolve_domain_planner",
]
