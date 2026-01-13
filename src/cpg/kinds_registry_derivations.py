"""Row-driven derivation registry helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from cpg.kinds_registry_enums import EdgeKind, NodeKind
from cpg.kinds_registry_models import DerivationSpec, DerivationStatus


@dataclass(frozen=True)
class DerivationTemplate:
    """Reusable defaults for derivation specs."""

    extractor: str
    confidence_policy: str
    ambiguity_policy: str
    status: DerivationStatus = "implemented"


@dataclass(frozen=True)
class DerivationRow[KindT: NodeKind | EdgeKind]:
    """Row-driven derivation spec definition."""

    kind: KindT
    provider_or_field: str
    id_recipe: str
    join_keys: tuple[str, ...] = ()
    template: DerivationTemplate | None = None
    extractor: str | None = None
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    status: DerivationStatus | None = None
    notes: str = ""


def build_derivations[KindT: NodeKind | EdgeKind](
    rows: Sequence[DerivationRow[KindT]],
) -> dict[KindT, list[DerivationSpec]]:
    """Build derivation specs grouped by kind.

    Returns
    -------
    dict[KindT, list[DerivationSpec]]
        Mapping of kinds to derivation specs.

    Raises
    ------
    ValueError
        If a derivation row is missing required fields after template resolution.
    """
    out: dict[KindT, list[DerivationSpec]] = {}
    for row in rows:
        template = row.template
        extractor = row.extractor or (template.extractor if template else None)
        if extractor is None:
            msg = f"Missing extractor for derivation row {row.kind.value}."
            raise ValueError(msg)
        confidence_policy = row.confidence_policy or (
            template.confidence_policy if template else None
        )
        if confidence_policy is None:
            msg = f"Missing confidence_policy for derivation row {row.kind.value}."
            raise ValueError(msg)
        ambiguity_policy = row.ambiguity_policy or (template.ambiguity_policy if template else None)
        if ambiguity_policy is None:
            msg = f"Missing ambiguity_policy for derivation row {row.kind.value}."
            raise ValueError(msg)
        status = row.status or (template.status if template else None)
        if status is None:
            msg = f"Missing status for derivation row {row.kind.value}."
            raise ValueError(msg)
        spec = DerivationSpec(
            extractor=extractor,
            provider_or_field=row.provider_or_field,
            join_keys=row.join_keys,
            id_recipe=row.id_recipe,
            confidence_policy=confidence_policy,
            ambiguity_policy=ambiguity_policy,
            status=status,
            notes=row.notes,
        )
        out.setdefault(row.kind, []).append(spec)
    return out


def flatten_derivations(
    derivations: Mapping[NodeKind | EdgeKind, Sequence[DerivationSpec]],
) -> tuple[DerivationSpec, ...]:
    """Flatten derivation mapping into a tuple.

    Returns
    -------
    tuple[DerivationSpec, ...]
        Flattened derivation specs.
    """
    specs: list[DerivationSpec] = []
    for items in derivations.values():
        specs.extend(items)
    return tuple(specs)


__all__ = [
    "DerivationRow",
    "DerivationTemplate",
    "build_derivations",
    "flatten_derivations",
]
