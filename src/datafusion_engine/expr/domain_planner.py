"""Domain planner specifications for DataFusion expression planners."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from datafusion_engine.udf.metadata import rewrite_tag_index


@dataclass(frozen=True)
class DomainPlannerSpec:
    """Specification for a domain expression planner.

    Domain planners enable custom operator syntax and named argument support
    for domain-specific query patterns.
    """

    name: str
    operator_names: tuple[str, ...] = ()
    rewrite_tags: tuple[str, ...] = ()

    def operator_names_from_snapshot(
        self, snapshot: Mapping[str, object] | None
    ) -> tuple[str, ...]:
        """Return operator names derived from registry rewrite tags.

        Returns:
        -------
        tuple[str, ...]
            Operator names enabled for this planner.
        """
        if snapshot is None or not self.rewrite_tags:
            return self.operator_names
        tag_index = rewrite_tag_index(snapshot)
        resolved = set(self.operator_names)
        for tag in self.rewrite_tags:
            resolved.update(tag_index.get(tag, ()))
        return tuple(sorted(resolved))

    def to_payload(self, *, snapshot: Mapping[str, object] | None = None) -> dict[str, object]:
        """Return a payload representation for the domain planner.

        Returns:
        -------
        dict[str, object]
            Domain planner payload mapping.
        """
        return {
            "name": self.name,
            "operator_names": list(self.operator_names_from_snapshot(snapshot)),
        }


CODEANATOMY_DOMAIN_PLANNER = DomainPlannerSpec(
    name="codeanatomy_domain",
    rewrite_tags=("hash", "position_encoding"),
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

    Returns:
    -------
    DomainPlannerSpec | None
        Domain planner spec when available.
    """
    return DEFAULT_DOMAIN_PLANNERS.get(name)


def domain_planner_payloads(
    planner_names: tuple[str, ...],
    *,
    snapshot: Mapping[str, object] | None = None,
) -> list[dict[str, object]]:
    """Return structured payloads for domain planners.

    Parameters
    ----------
    planner_names
        Tuple of domain planner names to resolve.
    snapshot
        Optional registry snapshot for rewrite tag resolution.

    Returns:
    -------
    list[dict[str, object]]
        Payload representations for each resolved planner.
    """
    payloads: list[dict[str, object]] = []
    for name in planner_names:
        planner = resolve_domain_planner(name)
        if planner is not None:
            payloads.append(planner.to_payload(snapshot=snapshot))
    return payloads


def domain_planner_names_from_snapshot(
    snapshot: Mapping[str, object] | None,
) -> tuple[str, ...]:
    """Return planner names enabled by registry rewrite tags.

    Returns:
    -------
    tuple[str, ...]
        Planner names enabled by the snapshot.
    """
    if snapshot is None:
        return ()
    tag_index = rewrite_tag_index(snapshot)
    enabled: list[str] = []
    for name, planner in DEFAULT_DOMAIN_PLANNERS.items():
        if not planner.rewrite_tags:
            continue
        if any(tag in tag_index for tag in planner.rewrite_tags):
            enabled.append(name)
    return tuple(enabled)


__all__ = [
    "CODEANATOMY_DOMAIN_PLANNER",
    "DEFAULT_DOMAIN_PLANNERS",
    "DomainPlannerSpec",
    "domain_planner_names_from_snapshot",
    "domain_planner_payloads",
    "resolve_domain_planner",
]
